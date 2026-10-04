package ru.quipy.payments.logic

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration

class PaymentAccountLimiter(
    parallelRequests: Int,
    rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    fun acquire(deadline: Long): Boolean {
        val latestStart = deadline - averageProcessingTime.toMillis()

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
