package ru.quipy.payments.logic

import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration

class PaymentAccountLimiter(
    parallelRequests: Int,
    rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    fun acquire(deadline: Long): Boolean {
        val latestStart = deadline - averageProcessingTime.toMillis()

        while (true) {
            val remaining = latestStart - System.currentTimeMillis()
            if (remaining <= 0) return false

            when (ongoingWindow.putIntoWindow()) {
                is NonBlockingOngoingWindow.WindowResponse.Success -> break
                is NonBlockingOngoingWindow.WindowResponse.Fail -> Thread.sleep(minOf(10, remaining))
            }
        }

        var acquired = false
        try {
            rateLimiter.tickBlocking()
            acquired = System.currentTimeMillis() < latestStart
            return acquired
        } finally {
            if (!acquired) ongoingWindow.releaseWindow()
        }
    }

    fun release() {
        ongoingWindow.releaseWindow()
    }
}
