package ru.quipy.common.utils

import java.time.Duration
import kotlin.math.ceil

class PacedSlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val clockMillis: () -> Long = System::currentTimeMillis,
    private val clockNanos: () -> Long = System::nanoTime,
) : RateLimiter {
    private val slidingWindow = SlidingWindowRateLimiter(rate, window, clockMillis)
    private val intervalNanos = ceil(window.toNanos().toDouble() / rate).toLong()
    private var nextSendNanos: Long? = null

    @Synchronized
    override fun tick(): Boolean {
        val now = clockNanos()
        if (nextSendNanos?.let { it - now > 0L } == true) return false
        if (!slidingWindow.tick()) return false
        nextSendNanos = now + intervalNanos
        return true
    }

    @Synchronized
    fun estimatePermissionTime(requestsAhead: Int, now: Long = clockMillis()): Long {
        require(requestsAhead >= 0)
        val remainingNanos = nextSendNanos?.let { (it - clockNanos()).coerceAtLeast(0L) } ?: 0L
        val pacedDelayMillis = ceil((remainingNanos.toDouble() + requestsAhead * intervalNanos.toDouble()) / 1_000_000).toLong()
        return maxOf(slidingWindow.estimatePermissionTime(requestsAhead, now), now + pacedDelayMillis)
    }
}
