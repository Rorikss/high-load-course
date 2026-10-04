package ru.quipy.payments.logic

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountLimiter(
    private val parallelRequests: Int,
    private val rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val pendingRequests = AtomicInteger()

    fun tryReserve(deadline: Long, processingBudget: Duration): Boolean {
        while (true) {
            val pending = pendingRequests.get()
            val parallelDelayMillis = (pending / parallelRequests) * processingBudget.toMillis()
            val rateDelayMillis = (pending / rateLimitPerSec) * 1_000L
            val estimatedFinish = System.currentTimeMillis() +
                maxOf(parallelDelayMillis, rateDelayMillis) +
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
