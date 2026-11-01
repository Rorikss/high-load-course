package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

class BurstSlidingWindowRateLimiterTest {
    @Test
    fun `account admission fits eleven fresh requests in short deadline`() {
        val limiter = ru.quipy.payments.logic.PaymentAccountLimiter(64, 11, burstCapacity = 6)
        repeat(11) {
            assertTrue(limiter.tryReserve(java.util.UUID.randomUUID(), System.currentTimeMillis() + 2500,
                Duration.ofSeconds(2), 16, Duration.ofSeconds(2), Duration.ofSeconds(1)))
        }
        assertFalse(limiter.tryReserve(java.util.UUID.randomUUID(), System.currentTimeMillis() + 2500,
            Duration.ofSeconds(2), 16, Duration.ofSeconds(2), Duration.ofSeconds(1)))
    }

    @Test
    fun `concurrent attempts cannot overspend burst`() {
        val limiter = BurstSlidingWindowRateLimiter(11, Duration.ofSeconds(1), 6) { 0L }
        val pool = java.util.concurrent.Executors.newFixedThreadPool(16)
        try {
            val results = pool.invokeAll(List(100) { java.util.concurrent.Callable { limiter.tick() } })
            assertEquals(6, results.count { it.get() })
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `six initial permits and eleven within half second`() {
        var nanos = 0L
        val limiter = BurstSlidingWindowRateLimiter(11, Duration.ofSeconds(1), 6) { nanos }
        assertEquals(0L, limiter.estimatePermissionTime(5, 0))
        assertEquals(455L, limiter.estimatePermissionTime(10, 0))
        repeat(6) { assertTrue(limiter.tick()) }
        assertFalse(limiter.tick())
        repeat(5) {
            nanos += 91_000_000L
            assertTrue(limiter.tick())
        }
        nanos = 999_000_000L
        assertFalse(limiter.tick())
        nanos = 1_000_000_000L
        assertTrue(limiter.tick())
    }

    @Test
    fun `forecast agrees with sequential execution and does not consume permits`() {
        var nanos = 0L
        val limiter = BurstSlidingWindowRateLimiter(11, Duration.ofSeconds(1), 6) { nanos }
        repeat(9) {
            while (!limiter.tick()) nanos += 100_000L
        }
        val predictions = (0..39).map { limiter.estimatePermissionTime(it, nanos / 1_000_000) }
        predictions.forEach { predicted ->
            while (!limiter.tick()) nanos += 100_000L
            assertTrue(kotlin.math.abs(predicted - nanos / 1_000_000) <= 2)
        }
    }

    @Test
    fun `full sliding window does not burn replenished tokens`() {
        var nanos = 0L
        val limiter = BurstSlidingWindowRateLimiter(11, Duration.ofSeconds(1), 6) { nanos }
        repeat(11) { while (!limiter.tick()) nanos += 1_000_000L }
        nanos = 999_000_000L
        repeat(100) { assertFalse(limiter.tick()) }
        nanos = 1_001_000_000L
        repeat(6) { assertTrue(limiter.tick()) }
        assertFalse(limiter.tick())
    }

    @Test
    fun `all rolling seconds respect account limit under irregular polling`() {
        var nanos = 0L
        val limiter = BurstSlidingWindowRateLimiter(11, Duration.ofSeconds(1), 6) { nanos }
        val sends = mutableListOf<Long>()
        repeat(15000) { i ->
            nanos += if (i % 137 == 0) 25_000_000L else 1_000_000L
            while (limiter.tick()) sends.add(nanos)
        }
        assertTrue(sends.size > 150)
        for (i in 11 until sends.size) assertTrue(sends[i] - sends[i - 11] >= 1_000_000_000L)
    }
}
