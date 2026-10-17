package ru.quipy.payments.logic

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class PaymentAccountLimiter(
    private val parallelRequests: Int,
    private val rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val pendingRequests = AtomicInteger()

    fun tryReserve(deadline: Long, processingBudget: Duration, executorParallelism: Int): Boolean {
        val effectiveParallelism = minOf(parallelRequests, executorParallelism)
        val processingMillis = processingBudget.toMillis().toDouble()
        val throughputPerMillis = minOf(rateLimitPerSec / 1_000.0, effectiveParallelism / processingMillis)
        while (true) {
            val pending = pendingRequests.get()
            val waitingMillis = maxOf(0.0, (pending + 1) / throughputPerMillis - processingMillis)
            val estimatedFinish = System.currentTimeMillis() +
                ceil(waitingMillis).toLong() +
                processingBudget.toMillis()

            if (estimatedFinish >= deadline) return false
            if (pendingRequests.compareAndSet(pending, pending + 1)) return true
        }
    }

    fun releaseReservation() {
        check(pendingRequests.decrementAndGet() >= 0) { "Payment reservation counter became negative" }
    }

    fun acquire(deadline: Long, processingBudget: Duration = averageProcessingTime): Boolean {
        val latestStart = deadline - processingBudget.toMillis()

        val remaining = latestStart - System.currentTimeMillis()
        if (remaining <= 0 || !ongoingWindow.tryAcquire(remaining)) return false

        var acquired = false
        try {
            rateLimiter.tickBlocking()
            acquired = System.currentTimeMillis() < latestStart
            return acquired
        } finally {
            if (!acquired) ongoingWindow.release()
        }
    }

    fun release() {
        ongoingWindow.release()
    }
}
