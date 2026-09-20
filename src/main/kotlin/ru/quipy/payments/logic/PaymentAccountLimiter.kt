package ru.quipy.payments.logic

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class PaymentAccountLimiter(
    parallelRequests: Int,
    rateLimitPerSec: Int,
    private val averageProcessingTime: Duration,
) {
    private val permits = Semaphore(parallelRequests, true)
    private val intervalNanos = 1_000_000_000L * 100 / (rateLimitPerSec * 98L)
    private val rateLock = ReentrantLock(true)
    private val rateChanged = rateLock.newCondition()
    private var nextStartAt = System.nanoTime()

    fun acquire(deadline: Long): Boolean {
        if (!acquirePermit(deadline)) {
            return false
        }

        var ready = false
        try {
            ready = awaitRateSlot(deadline)
            return ready
        } finally {
            if (!ready) {
                permits.release()
            }
        }
    }

    private fun acquirePermit(deadline: Long): Boolean {
        val remaining = remainingNanos(deadline)
        return remaining > 0 && permits.tryAcquire(remaining, TimeUnit.NANOSECONDS)
    }

    private fun awaitRateSlot(deadline: Long): Boolean {
        val remaining = remainingNanos(deadline)
        if (remaining <= 0 || !rateLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {
            return false
        }

        try {
            while (true) {
                val timeLeft = remainingNanos(deadline)
                if (timeLeft <= 0) {
                    return false
                }

                val now = System.nanoTime()
                val waitNanos = nextStartAt - now
                if (waitNanos <= 0) {
                    nextStartAt = now + intervalNanos
                    return true
                }

                if (waitNanos >= timeLeft) {
                    return false
                }

                rateChanged.awaitNanos(waitNanos)
            }
        } finally {
            rateLock.unlock()
        }
    }

    private fun remainingNanos(deadline: Long): Long {
        val remainingMillis = deadline - System.currentTimeMillis() - averageProcessingTime.toMillis()
        return TimeUnit.MILLISECONDS.toNanos(remainingMillis)
    }

    fun release() {
        permits.release()
    }
}
