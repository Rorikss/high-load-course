package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

class PacedSlidingWindowRateLimiterTest {
    @Test
    fun `saturated flow approaches eleven per second without bursts`() {
        var nanos = 0L
        val limiter = PacedSlidingWindowRateLimiter(11, Duration.ofSeconds(1), { nanos / 1_000_000 }, { nanos })
        val sends = mutableListOf<Long>()
        repeat(110) {
            while (!limiter.tick()) nanos += 1_000_000
            sends.add(nanos)
            assertFalse(limiter.tick())
        }
        assertTrue(sends.last() < 10_000_000_000L)
        for (i in 11 until sends.size) {
            assertTrue(sends[i] - sends[i - 11] >= 1_000_000_000L)
        }
    }

    @Test
    fun `pause does not accumulate burst credits`() {
        var nanos = 0L
        val limiter = PacedSlidingWindowRateLimiter(11, Duration.ofSeconds(1), { nanos / 1_000_000 }, { nanos })
        assertTrue(limiter.tick())
        nanos = 10_000_000_000L
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        assertEquals(10091L, limiter.estimatePermissionTime(0))
        assertEquals(10182L, limiter.estimatePermissionTime(1))
    }

    @Test
    fun `rejected attempts do not consume next permit`() {
        var nanos = 0L
        val limiter = PacedSlidingWindowRateLimiter(11, Duration.ofSeconds(1), { nanos / 1_000_000 }, { nanos })
        assertTrue(limiter.tick())
        repeat(100) { assertFalse(limiter.tick()) }
        nanos = 91_000_000L
        assertTrue(limiter.tick())
    }
}
