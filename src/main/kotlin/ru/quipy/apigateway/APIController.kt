package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.exceptions.TooManyRequestsException
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentAccountProperties
import java.util.*
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import kotlin.math.round

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var accountProperties: PaymentAccountProperties

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    @PostConstruct
    fun registerRateLimiterMetrics() {
        Gauge.builder("payment.input.tokenbucket.tokens", tokenBucketRateLimiter) {
            it.availableTokens().toDouble()
        }
            .description("Current number of available tokens in the input token bucket")
            .register(meterRegistry)
    }

    //    private val paymentRateLimiter: TokenBucketRateLimiter by lazy {
//        TokenBucketRateLimiter(
//            rate = 3,
//            bucketMaxCapacity = 300,
//            startBucket = 300,
//            window = 1000,
//            timeUnit = TimeUnit.MILLISECONDS,
//        )
//    }
    private val tokenBucketRateLimiter: TokenBucketRateLimiter by lazy {
        val windowMs = 1_000L
        val rps = accountProperties.rateLimitPerSec
        val burst = accountProperties.maxBurst

        TokenBucketRateLimiter(
            rate = (rps * windowMs / 1_000).toInt(),  // = rps при window=1000мс
            bucketMaxCapacity = burst,
            startBucket = burst,                       // полный запас с самого старта
            window = windowMs,
            timeUnit = TimeUnit.MILLISECONDS,
        )
    }

//    private val paymentRateLimiter: CompositeRateLimiter by lazy {
//        val windowMs = 1_000L
//        val rps = accountProperties.rateLimitPerSec
//        val burst = accountProperties.maxBurst

//        CompositeRateLimiter(
//            tokenBucketRateLimiter,
//            LeakingBucketRateLimiter(
//                rate = rps.toLong(),
//                window = java.time.Duration.ofSeconds(1),
//                bucketSize = round(rps*1.2).toInt(),
//            ),
//            CompositeRateLimiter.Mode.AND,
//        )
//    }

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<Any> {
        val receivedAt = System.currentTimeMillis()
        if (!tokenBucketRateLimiter.tick()) {
            throw TooManyRequestsException(retryAfterMillisecond = 100)
        }
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId) ?: throw IllegalArgumentException("No such order $orderId")
        val createdAt = try {
            orderPayer.processPayment(orderId, order.price, paymentId, deadline, receivedAt)
        } catch (e: RejectedExecutionException) {
            val retryAt = System.currentTimeMillis() + 1_000
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, retryAt.toString())
                .build()
        }

        orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
