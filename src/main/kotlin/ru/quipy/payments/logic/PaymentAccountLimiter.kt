package ru.quipy.payments.logic

import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountLimiter(
    private val parallelRequests: Int,
    private val rateLimitPerSec: Int,
    rpsWindowMargin: Duration = Duration.ZERO,
    private val priorityArrivalWindow: Duration = Duration.ZERO,
) {
    init {
        require(!rpsWindowMargin.isNegative)
        require(!priorityArrivalWindow.isNegative)
    }

    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1).plus(rpsWindowMargin))
    private val pendingRequests = AtomicInteger()
    private data class Reservation(val latestStart: Long, val amount: Int, val receivedAt: Long)

    private val submissionOrder = linkedMapOf<UUID, Reservation>()
    private val readyPayments = mutableSetOf<UUID>()
    private var executingPayments = 0

    @Synchronized
    fun tryReserve(paymentId: UUID, deadline: Long, processingBudget: Duration, executorParallelism: Int, workerBudget: Duration = processingBudget, workerMean: Duration = workerBudget, amount: Int = 0, receivedAt: Long = System.currentTimeMillis()): Boolean {
        require(workerBudget >= processingBudget)
        val effectiveParallelism = minOf(parallelRequests, executorParallelism)
        val workerMillis = workerBudget.toMillis().toDouble()
        val workerFlowMillis = workerMean.toMillis().coerceAtLeast(1L).toDouble()
        val throughputPerMillis = effectiveParallelism / workerFlowMillis
        val pending = pendingRequests.get()
        val waitingMillis = maxOf(0.0, (pending + 1) / throughputPerMillis - workerFlowMillis)
        val waitingSafetyMarginMillis = if (pending >= effectiveParallelism) workerMillis / effectiveParallelism + 1_000.0 / rateLimitPerSec else 0.0
        val now = System.currentTimeMillis()
        val rpsAvailableAt = rateLimiter.estimatePermissionTime(pending - executingPayments, now)
        val estimatedFinish = maxOf(
            now + waitingMillis + waitingSafetyMarginMillis,
            rpsAvailableAt.toDouble(),
        ) + processingBudget.toMillis()

        if (estimatedFinish >= deadline) return false
        pendingRequests.incrementAndGet()
        submissionOrder[paymentId] = Reservation(deadline - processingBudget.toMillis(), amount, receivedAt)
        return true
    }

    @Synchronized
    fun releaseReservation(paymentId: UUID) {
        submissionOrder.remove(paymentId)
        readyPayments.remove(paymentId)
        check(pendingRequests.decrementAndGet() >= 0) { "Payment reservation counter became negative" }
        (this as java.lang.Object).notifyAll()
    }

    @Synchronized
    fun acquire(paymentId: UUID): Boolean {
        readyPayments.add(paymentId)
        (this as java.lang.Object).notifyAll()
        try {
            while (true) {
                val now = System.currentTimeMillis()
                while (submissionOrder.entries.firstOrNull()?.value?.latestStart?.let { it <= now } == true) {
                    submissionOrder.remove(submissionOrder.keys.first())
                    (this as java.lang.Object).notifyAll()
                }
                val latestStart = submissionOrder[paymentId]?.latestStart ?: return false
                if (latestStart <= now) return false
                var wakeAt = minOf(latestStart, submissionOrder.values.first().latestStart)
                if (nextPayment(now) == paymentId && ongoingWindow.tryAcquire(0)) {
                    if (rateLimiter.tick()) {
                        submissionOrder.remove(paymentId)
                        executingPayments++
                        (this as java.lang.Object).notifyAll()
                        return true
                    }
                    ongoingWindow.release()
                    wakeAt = minOf(wakeAt, rateLimiter.estimatePermissionTime(0, System.currentTimeMillis()))
                }
                val waitMillis = latestStart - System.currentTimeMillis()
                if (waitMillis <= 0) return false
                (this as java.lang.Object).wait(maxOf(1L, wakeAt - System.currentTimeMillis()))
            }
        } finally {
            readyPayments.remove(paymentId)
            (this as java.lang.Object).notifyAll()
        }
    }

    private fun nextPayment(now: Long): UUID {
        val first = submissionOrder.entries.first()
        if (priorityArrivalWindow.isZero || first.key !in readyPayments) return first.key
        val candidates = submissionOrder.entries.takeWhile {
            it.key in readyPayments &&
                it.value.latestStart > now &&
                it.value.receivedAt <= first.value.receivedAt + priorityArrivalWindow.toMillis()
        }.take((parallelRequests - executingPayments).coerceAtLeast(0))

        return candidates.withIndex()
            .sortedWith(compareByDescending<IndexedValue<Map.Entry<UUID, Reservation>>> { it.value.value.amount }
                .thenBy { it.index })
            .firstOrNull { candidate ->
                rateLimiter.estimatePermissionTime(0, now) < candidate.value.value.latestStart &&
                    (0 until candidate.index).all { position ->
                        rateLimiter.estimatePermissionTime(position + 1, now) < candidates[position].value.latestStart
                    }
            }?.value?.key ?: first.key
    }

    @Synchronized
    fun release() {
        check(executingPayments > 0)
        executingPayments--
        ongoingWindow.release()
        (this as java.lang.Object).notifyAll()
    }
}
