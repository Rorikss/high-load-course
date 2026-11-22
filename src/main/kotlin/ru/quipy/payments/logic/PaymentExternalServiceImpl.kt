package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
    private val admissionP99Window: Duration,
    private val maxRetries: Int,
    private val attemptTimeout: Duration,
) : PaymentExternalSystemAdapter {

    init {
        require(maxRetries >= 0) { "payment.max-retries must be non-negative" }
        require(!attemptTimeout.isZero && !attemptTimeout.isNegative) { "payment.attempt-timeout must be positive" }
    }

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val limiter = PaymentAccountLimiter(parallelRequests, rateLimitPerSec, requestAverageProcessingTime)
    private val startedAt = now()
    private val admittedCounter = admissionCounter(meterRegistry, "admitted")
    private val deadlineRejectedCounter = admissionCounter(meterRegistry, "deadline_rejected")
    private val deadlineMissedCounter = admissionCounter(meterRegistry, "deadline_missed")
    private val retryCounter = Counter.builder("payment.retry.attempts")
        .description("Number of external payment calls actually sent after the initial attempt")
        .tags("service", serviceName, "account", accountName)
        .register(meterRegistry)
    private val timeoutCounter = Counter.builder("payment.attempt.outcomes")
        .description("Number of timed out external payment attempts")
        .tags("service", serviceName, "account", accountName, "outcome", "timeout")
        .register(meterRegistry)
    private val limiterAdmittedWaitTimer = stageTimer(meterRegistry, "limiter_wait", "admitted")
    private val limiterMissedWaitTimer = stageTimer(meterRegistry, "limiter_wait", "deadline_missed")
    private val externalCallTimer = stageTimer(
        meterRegistry,
        "external_call",
        "finished",
        admissionP99Window,
    )
    private val totalSuccessTimer = totalDurationTimer(meterRegistry, "success")
    private val totalFailedTimer = totalDurationTimer(meterRegistry, "failed")
    private val totalDeadlineMissedTimer = totalDurationTimer(meterRegistry, "deadline_missed")

    private val client = OkHttpClient.Builder()
        .callTimeout(attemptTimeout)
        .build()

    init {
        Gauge.builder("payment.external.timeout.seconds", this) { it.calculateAttemptTimeout().toMillis() / 1000.0 }
            .description("Adaptive timeout budget of one HTTP attempt before the deadline cap")
            .tags("service", serviceName, "account", accountName)
            .register(meterRegistry)
    }

    override fun tryReserve(deadline: Long, executorParallelism: Int): Boolean {
        val reserved = limiter.tryReserve(deadline, processingBudget(), executorParallelism)
        if (reserved) admittedCounter.increment() else deadlineRejectedCounter.increment()
        return reserved
    }

    override fun releaseReservation() {
        limiter.releaseReservation()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        var totalDurationRecorded = false

        fun recordTotalDuration(timer: Timer) {
            if (!totalDurationRecorded) {
                timer.record(Duration.ofMillis(now() - paymentStartedAt))
                totalDurationRecorded = true
            }
        }

        try {
            val submittedAt = now()
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }

            val retry = Retry.of("payment-$accountName", RetryConfig.custom<ExternalSysResponse?>()
                .maxAttempts(Math.addExact(maxRetries, 1))
                .waitDuration(Duration.ZERO)
                .retryOnException { it is InterruptedIOException && now() + processingBudget().toMillis() < deadline }
                .retryOnResult { response ->
                    response != null && !response.result && response.message == "Temporary error" &&
                        now() + processingBudget().toMillis() < deadline
                }
                .build())
            var attempt = 0

            val body = retry.executeCallable<ExternalSysResponse?> {
                val attemptBudget = calculateAttemptTimeout()
                val limiterStartedAt = now()
                val acquired = try {
                    limiter.acquire(deadline, processingBudget())
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    false
                }
                val limiterWait = Duration.ofMillis(now() - limiterStartedAt)

                if (!acquired) {
                    limiterMissedWaitTimer.record(limiterWait)
                    deadlineMissedCounter.increment()
                    val rejectedAt = now()
                    paymentESService.update(paymentId) {
                        it.logSubmission(false, transactionId, rejectedAt, Duration.ofMillis(rejectedAt - paymentStartedAt))
                    }
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded before submission.")
                    }
                    recordTotalDuration(totalDeadlineMissedTimer)
                    return@executeCallable null
                }

                try {
                    limiterAdmittedWaitTimer.record(limiterWait)

                    logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    if (attempt++ > 0) retryCounter.increment()

                    val externalStartedAt = now()
                    try {
                        val call = client.newCall(request)
                        call.timeout().timeout(minOf(attemptBudget.toMillis(), deadline - now()), TimeUnit.MILLISECONDS)
                        val response = call.execute()
                        val body = response.use {
                            try {
                                mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                if (e is InterruptedIOException) throw e
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.code}, reason: ${e.message}", e)
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                        }
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                        body
                    } catch (e: InterruptedIOException) {
                        timeoutCounter.increment()
                        throw e
                    } finally {
                        externalCallTimer.record(Duration.ofMillis(now() - externalStartedAt))
                    }
                } finally {
                    limiter.release()
                }
            } ?: return
            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
            recordTotalDuration(if (body.result) totalSuccessTimer else totalFailedTimer)
        } catch (e: Exception) {
            recordTotalDuration(totalFailedTimer)
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun processingBudget(): Duration {
        val fallback = requestAverageProcessingTime.multipliedBy(2)
        if (now() - startedAt < admissionP99Window.toMillis()) return fallback

        val p99Millis = externalCallTimer.takeSnapshot()
            .percentileValues()
            .firstOrNull { it.percentile() == 0.99 }
            ?.value(TimeUnit.MILLISECONDS)
            ?.takeIf { it > 0.0 }
            ?: return fallback

        return Duration.ofMillis(ceil(p99Millis).toLong())
    }

    private fun calculateAttemptTimeout(): Duration {
        if (externalCallTimer.count() < 100) return attemptTimeout

        val p85Millis = externalCallTimer.takeSnapshot()
            .percentileValues()
            .firstOrNull { it.percentile() == 0.85 }
            ?.value(TimeUnit.MILLISECONDS)
            ?.takeIf { it.isFinite() && it > 0.0 }
            ?: return attemptTimeout

        val lowerBound = minOf(attemptTimeout, requestAverageProcessingTime)
        val upperBound = maxOf(attemptTimeout, requestAverageProcessingTime.multipliedBy(2))
        return Duration.ofMillis(ceil(p85Millis * 1.5).toLong())
            .coerceIn(lowerBound, upperBound)
    }

    private fun admissionCounter(meterRegistry: MeterRegistry, result: String): Counter =
        Counter.builder("payment.admission")
            .description("Payments admitted to or rejected by the account limiter")
            .tag("service", serviceName)
            .tag("account", accountName)
            .tag("result", result)
            .register(meterRegistry)

    private fun stageTimer(
        meterRegistry: MeterRegistry,
        stage: String,
        result: String,
        percentileWindow: Duration? = null,
    ): Timer {
        val builder = Timer.builder("payment.stage.duration")
            .description("Duration of a payment processing stage")
            .tags(
                "service", serviceName,
                "account", accountName,
                "stage", stage,
                "result", result,
            )
            .publishPercentileHistogram()
            .serviceLevelObjectives(*durationBuckets())

        if (percentileWindow != null) {
            builder
                .publishPercentiles(0.85, 0.99)
                .distributionStatisticExpiry(percentileWindow)
                .distributionStatisticBufferLength(1)
        }

        return builder.register(meterRegistry)
    }

    private fun totalDurationTimer(meterRegistry: MeterRegistry, result: String): Timer =
        Timer.builder("payment.total.duration")
            .description("Time from accepting a payment request to its terminal result")
            .tags(
                "service", serviceName,
                "account", accountName,
                "result", result,
            )
            .publishPercentileHistogram()
            .serviceLevelObjectives(*durationBuckets())
            .register(meterRegistry)

    private fun durationBuckets(): Array<Duration> = arrayOf(
        Duration.ofMillis(100),
        Duration.ofSeconds(1),
        Duration.ofSeconds(5),
        Duration.ofSeconds(10),
        Duration.ofSeconds(20),
        Duration.ofSeconds(30),
        Duration.ofSeconds(40),
        Duration.ofSeconds(50),
        Duration.ofSeconds(60),
    )

}

public fun now() = System.currentTimeMillis()
