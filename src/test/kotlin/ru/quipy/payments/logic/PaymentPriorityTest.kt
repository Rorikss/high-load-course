package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

class PaymentPriorityTest {
    @Test
    fun `nearby expensive payment goes first without losing either payment`() {
        assertEquals(listOf("expensive", "first"), runPair())
    }

    @Test
    fun `urgent earlier payment cannot be displaced`() {
        assertEquals(listOf("first", "expensive"), runPair(firstDeadlineMillis = 800))
    }

    @Test
    fun `payment from later arrival group cannot overtake`() {
        assertEquals(listOf("first", "expensive"), runPair(arrivalGapMillis = 20))
    }

    @Test
    fun `equal prices retain FIFO`() {
        assertEquals(listOf("first", "expensive"), runPair(secondAmount = 100))
    }

    @Test
    fun `zero window disables priority`() {
        assertEquals(listOf("first", "expensive"), runPair(priorityWindowMillis = 0))
    }

    @Test
    fun `no spare account concurrency keeps FIFO`() {
        assertEquals(listOf("first", "expensive"), runPair(parallelism = 1))
    }

    private fun runPair(
        firstDeadlineMillis: Long = 5_000,
        arrivalGapMillis: Long = 3,
        secondAmount: Int = 300,
        priorityWindowMillis: Long = 19,
        parallelism: Int = 16,
    ): List<String> {
        val limiter = PaymentAccountLimiter(parallelism, 1, Duration.ZERO, Duration.ofMillis(priorityWindowMillis))
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        val now = System.currentTimeMillis()
        val receivedAt = now - arrivalGapMillis
        val budget = Duration.ofMillis(100)
        assertTrue(limiter.tryReserve(first, now + firstDeadlineMillis, budget, 16, amount = 100, receivedAt = receivedAt))
        assertTrue(limiter.tryReserve(second, now + 5_000, budget, 16, amount = secondAmount, receivedAt = now))
        val order = CopyOnWriteArrayList<String>()
        val failures = CopyOnWriteArrayList<Throwable>()

        fun worker(id: UUID, label: String) = Thread {
            try {
                assertTrue(limiter.acquire(id))
                try {
                    order.add(label)
                } finally {
                    limiter.release()
                }
            } catch (e: Throwable) {
                failures.add(e)
            } finally {
                limiter.releaseReservation(id)
            }
        }

        val later = worker(second, "expensive")
        val earlier = worker(first, "first")
        try {
            later.start()
            val waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            while (later.state != Thread.State.TIMED_WAITING && System.nanoTime() < waitUntil) {
                Thread.sleep(1)
            }
            assertEquals(Thread.State.TIMED_WAITING, later.state)
            assertTrue(order.isEmpty(), "A waiting worker must not overtake a head that has not reached the limiter")
            earlier.start()
            earlier.join(3_000)
            later.join(3_000)
            assertFalse(earlier.isAlive)
            assertFalse(later.isAlive)
            assertTrue(failures.isEmpty(), failures.toString())
            return order.toList()
        } finally {
            earlier.interrupt()
            later.interrupt()
            earlier.join(1_000)
            later.join(1_000)
        }
    }
}
