package ru.quipy.payments.logic

import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentAccountLimiter(
    parallelRequests: Int,
    rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val permits = Semaphore(parallelRequests, true)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    fun acquire(deadline: Long): Boolean {
        val latestStart = deadline - averageProcessingTime.toMillis()
        val remaining = latestStart - System.currentTimeMillis()
        if (remaining <= 0 || !permits.tryAcquire(remaining, TimeUnit.MILLISECONDS)) return false

        var acquired = false
        try {
            rateLimiter.tickBlocking()
            acquired = System.currentTimeMillis() < latestStart
            return acquired
        } finally {
            if (!acquired) permits.release()
        }
    }

    fun release() {
        permits.release()
    }
}
