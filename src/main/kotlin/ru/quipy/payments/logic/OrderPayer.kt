package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    @Value("\${payment.queue-capacity:8000}") queueCapacity: Int,
    @Value("\${payment.service-name}") serviceName: String,
    meterRegistry: MeterRegistry,
    @Value("\${payment.admission.p99-window:5m}") p99Window: Duration,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentAccounts: List<PaymentExternalSystemAdapter>

    init {
        require(queueCapacity > 0) { "payment.queue-capacity must be positive" }
    }

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    init {
        Gauge.builder("payment.executor.queue.size", paymentExecutor) { it.queue.size.toDouble() }
            .description("Number of payment tasks waiting in the executor queue")
            .tag("service", serviceName)
            .register(meterRegistry)
    }

    private val queueWaitTimer = Timer.builder("payment.stage.duration")
        .description("Duration of a payment processing stage")
        .tags(
            "service", serviceName,
            "account", "all",
            "stage", "executor_queue",
            "result", "started",
        )
        .publishPercentileHistogram()
        .serviceLevelObjectives(*durationBuckets())
        .register(meterRegistry)

    private val workerBusyTimer = Timer.builder("payment.stage.duration")
        .description("Time a worker is occupied, excluding the executor queue")
        .tags("service", serviceName, "account", "all", "stage", "worker_busy", "result", "finished")
        .publishPercentileHistogram()
        .publishPercentiles(0.99)
        .distributionStatisticExpiry(p99Window)
        .distributionStatisticBufferLength(1)
        .serviceLevelObjectives(*durationBuckets())
        .register(meterRegistry)

    @Synchronized
    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long, receivedAt: Long): Long {
        val paymentAccount = paymentAccounts.single()
        if (!paymentAccount.tryReserve(paymentId, deadline, paymentExecutor.maximumPoolSize, amount, receivedAt)) {
            throw RejectedExecutionException("Payment cannot be completed before deadline")
        }

        try {
            paymentExecutor.submit {
                val workerStartedAt = System.nanoTime()
                try {
                    queueWaitTimer.record(Duration.ofMillis(System.currentTimeMillis() - receivedAt))

                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, receivedAt, deadline)
                } finally {
                    workerBusyTimer.record(System.nanoTime() - workerStartedAt, TimeUnit.NANOSECONDS)
                    paymentAccount.releaseReservation(paymentId)
                }
            }
        } catch (e: RejectedExecutionException) {
            paymentAccount.releaseReservation(paymentId)
            throw e
        }
        return receivedAt
    }

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
