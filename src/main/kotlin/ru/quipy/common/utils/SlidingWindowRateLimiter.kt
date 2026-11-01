package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
    private val clockMillis: () -> Long = System::currentTimeMillis,
) : RateLimiter {
    private val timestamps = ArrayDeque<Long>()

    init {
        require(rate in 1..Int.MAX_VALUE.toLong())
        require(window.toMillis() > 0)
    }

    @Synchronized
    override fun tick(): Boolean {
        val now = clockMillis()
        while (timestamps.isNotEmpty() && timestamps.first() <= now - window.toMillis()) {
            timestamps.removeFirst()
        }
        if (timestamps.size >= rate) return false
        timestamps.addLast(now)
        return true
    }

    fun tickBlocking(latestStart: Long): Boolean {
        while (true) {
            val remaining = latestStart - clockMillis()
            if (remaining <= 0) return false
            if (tick()) return true
            Thread.sleep(minOf(10L, remaining))
        }
    }

    @Synchronized
    fun estimatePermissionTime(requestsAhead: Int, now: Long = clockMillis()): Long {
        require(requestsAhead >= 0)
        val active = timestamps.filter { it + window.toMillis() > now }
        val available = rate.toInt() - active.size
        if (requestsAhead < available) return now
        val position = requestsAhead - available
        val previous = active.getOrElse(position % rate.toInt()) { now }
        return previous + (position / rate + 1) * window.toMillis()
    }
}
