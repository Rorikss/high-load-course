package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class BurstSlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    burstCapacity: Int,
    private val clockNanos: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val capacity: Long
    private data class State(var credits: Long, var updatedAt: Long, val sends: ArrayDeque<Long>)
    private val state: State

    init {
        require(rate > 0 && windowNanos > 0)
        require(burstCapacity > 0 && burstCapacity <= rate)
        capacity = Math.multiplyExact(windowNanos, burstCapacity.toLong())
        state = State(capacity, clockNanos(), ArrayDeque())
    }

    private fun ceilDivide(value: Long, divisor: Long): Long =
        value / divisor + if (value % divisor == 0L) 0L else 1L

    private fun advance(state: State, now: Long) {
        val elapsed = (now - state.updatedAt).coerceAtLeast(0L)
        val missing = capacity - state.credits
        state.credits = if (elapsed >= ceilDivide(missing, rate)) capacity else state.credits + elapsed * rate
        state.updatedAt = now
        while (state.sends.isNotEmpty() && now - state.sends.first() >= windowNanos) state.sends.removeFirst()
    }

    private fun availableAt(state: State, now: Long): Long {
        val tokenWait = ceilDivide((windowNanos - state.credits).coerceAtLeast(0L), rate)
        val windowWait = if (state.sends.size.toLong() >= rate) windowNanos - (now - state.sends.first()) else 0L
        return now + maxOf(tokenWait, windowWait)
    }

    private fun consume(state: State, now: Long) {
        state.credits -= windowNanos
        state.sends.addLast(now)
    }

    @Synchronized
    override fun tick(): Boolean {
        val now = clockNanos()
        advance(state, now)
        if (availableAt(state, now) != now) return false
        consume(state, now)
        return true
    }

    @Synchronized
    fun estimatePermissionTime(requestsAhead: Int, now: Long = System.currentTimeMillis()): Long {
        require(requestsAhead >= 0)
        val started = clockNanos()
        var time = started
        val forecast = State(state.credits, state.updatedAt, ArrayDeque(state.sends))
        repeat(requestsAhead + 1) {
            advance(forecast, time)
            time = availableAt(forecast, time)
            advance(forecast, time)
            consume(forecast, time)
        }
        return now + ceilDivide(time - started, 1_000_000L)
    }
}
