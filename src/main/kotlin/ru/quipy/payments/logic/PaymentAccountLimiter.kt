package ru.quipy.payments.logic

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountLimiter(
    private val parallelRequests: Int,
    private val rateLimitPerSec: Int,
    rpsWindowMargin: Duration = Duration.ZERO,
) {
    init {
        require(!rpsWindowMargin.isNegative)
    }

    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1).plus(rpsWindowMargin))
    private val pendingRequests = AtomicInteger()
    private val submissionOrder = linkedMapOf<UUID, Long>()
    private var executingPayments = 0

    @Synchronized
    fun tryReserve(paymentId: UUID, deadline: Long, processingBudget: Duration, executorParallelism: Int, workerBudget: Duration = processingBudget, workerMean: Duration = workerBudget): Boolean {
        require(workerBudget >= processingBudget)
        val effectiveParallelism = minOf(parallelRequests, executorParallelism)
        val workerMillis = workerBudget.toMillis().toDouble()
        val workerFlowMillis = workerMean.toMillis().coerceAtLeast(1L).toDouble()
        val throughputPerMillis = effectiveParallelism / workerFlowMillis
        val pending = pendingRequests.get()
        val waitingMillis = maxOf(0.0, (pending + 1) / throughputPerMillis - workerFlowMillis)
        val waitingSafetyMarginMillis = if (pending >= effectiveParallelism) workerMillis / effectiveParallelism + 1_000.0 / rateLimitPerSec else 0.0
        val now = System.currentTimeMillis()
        val rpsAvailableAt = rateLimiter.estimatePermissionTime(pending - executingPayments, now)
        val estimatedFinish = maxOf(
            now + waitingMillis + waitingSafetyMarginMillis,
            rpsAvailableAt.toDouble(),
        ) + processingBudget.toMillis()

        if (estimatedFinish >= deadline) return false
        pendingRequests.incrementAndGet()
        submissionOrder[paymentId] = deadline - processingBudget.toMillis()
        return true
    }

    @Synchronized
    fun releaseReservation(paymentId: UUID) {
        submissionOrder.remove(paymentId)
        check(pendingRequests.decrementAndGet() >= 0) { "Payment reservation counter became negative" }
        (this as java.lang.Object).notifyAll()
    }

    @Synchronized
    fun acquire(paymentId: UUID): Boolean {
        while (true) {
            val now = System.currentTimeMillis()
            while (submissionOrder.entries.firstOrNull()?.value?.let { it <= now } == true) {
                submissionOrder.remove(submissionOrder.keys.first())
                (this as java.lang.Object).notifyAll()
            }
            val latestStart = submissionOrder[paymentId] ?: return false
            var wakeAt = minOf(latestStart, submissionOrder.values.first())
            if (submissionOrder.keys.first() == paymentId && ongoingWindow.tryAcquire(0)) {
                if (rateLimiter.tick()) {
                    submissionOrder.remove(paymentId)
                    executingPayments++
                    (this as java.lang.Object).notifyAll()
                    return true
                }
                ongoingWindow.release()
                wakeAt = minOf(wakeAt, rateLimiter.estimatePermissionTime(0, System.currentTimeMillis()))
            }
            val waitMillis = latestStart - System.currentTimeMillis()
            if (waitMillis <= 0) return false
            (this as java.lang.Object).wait(maxOf(1L, wakeAt - System.currentTimeMillis()))
        }
    }

    @Synchronized
    fun release() {
        check(executingPayments > 0)
        executingPayments--
        ongoingWindow.release()
        (this as java.lang.Object).notifyAll()
    }
}
