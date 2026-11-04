package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.exceptions.TooManyRequestsException
import ru.quipy.common.utils.TokenBucketRateLimiter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentAccountProperties
import java.util.*
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import kotlin.math.ceil

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

    private val inputLimiter: TokenBucketRateLimiter by lazy {
        val capacity = accountProperties.rateLimitPerSec * 24
        TokenBucketRateLimiter(
            rate = accountProperties.rateLimitPerSec,
            bucketMaxCapacity = capacity,
            startBucket = capacity,
            window = 1,
            timeUnit = TimeUnit.SECONDS,
        )
    }

    @PostConstruct
    fun registerInputLimiter() {
        Gauge.builder("payment.input.tokenbucket.tokens", inputLimiter) { it.availableTokens().toDouble() }
            .description("Available permits for incoming payment attempts")
            .tags("service", accountProperties.serviceName, "account", accountProperties.accountName)
            .register(meterRegistry)
    }

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
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId) ?: throw IllegalArgumentException("No such order $orderId")
        if (!inputLimiter.tick()) {
            throw TooManyRequestsException(
                ceil(1000.0 / accountProperties.rateLimitPerSec).toInt()
            )
        }
        val createdAt = try {
            orderPayer.processPayment(orderId, order.price, paymentId, deadline, receivedAt)
        } catch (e: RejectedExecutionException) {
            throw TooManyRequestsException(
                ceil(1000.0 / accountProperties.rateLimitPerSec).toInt()
            )
        }

        orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
        return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
