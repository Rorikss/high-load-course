package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

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
    private val admittedCounter = admissionCounter(meterRegistry, "admitted")
    private val deadlineRejectedCounter = admissionCounter(meterRegistry, "deadline_rejected")
    private val limiterAdmittedWaitTimer = stageTimer(meterRegistry, "limiter_wait", "admitted")
    private val limiterRejectedWaitTimer = stageTimer(meterRegistry, "limiter_wait", "deadline_rejected")
    private val externalCallTimer = stageTimer(meterRegistry, "external_call", "finished")
    private val totalSuccessTimer = totalDurationTimer(meterRegistry, "success")
    private val totalFailedTimer = totalDurationTimer(meterRegistry, "failed")
    private val totalDeadlineRejectedTimer = totalDurationTimer(meterRegistry, "deadline_rejected")

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        var acquired = false
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

            val limiterStartedAt = now()
            acquired = try {
                limiter.acquire(deadline)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
            val limiterWait = Duration.ofMillis(now() - limiterStartedAt)

            if (!acquired) {
                limiterRejectedWaitTimer.record(limiterWait)
                deadlineRejectedCounter.increment()
                val rejectedAt = now()
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, rejectedAt, Duration.ofMillis(rejectedAt - paymentStartedAt))
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Payment deadline exceeded before submission.")
                }
                recordTotalDuration(totalDeadlineRejectedTimer)
                return
            }

            limiterAdmittedWaitTimer.record(limiterWait)
            admittedCounter.increment()

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val externalStartedAt = now()
            val response = try {
                client.newCall(request).execute()
            } finally {
                externalCallTimer.record(Duration.ofMillis(now() - externalStartedAt))
            }
            response.use {
                val body = try {
                    mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.code}, reason: ${it.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                recordTotalDuration(if (body.result) totalSuccessTimer else totalFailedTimer)
            }
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
        } finally {
            if (acquired) limiter.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun admissionCounter(meterRegistry: MeterRegistry, result: String): Counter =
        Counter.builder("payment.admission")
            .description("Payments admitted to or rejected by the account limiter")
            .tag("service", serviceName)
            .tag("account", accountName)
            .tag("result", result)
            .register(meterRegistry)

    private fun stageTimer(meterRegistry: MeterRegistry, stage: String, result: String): Timer =
        Timer.builder("payment.stage.duration")
            .description("Duration of a payment processing stage")
            .tags(
                "service", serviceName,
                "account", accountName,
                "stage", stage,
                "result", result,
            )
            .publishPercentileHistogram()
            .serviceLevelObjectives(*durationBuckets())
            .register(meterRegistry)

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
