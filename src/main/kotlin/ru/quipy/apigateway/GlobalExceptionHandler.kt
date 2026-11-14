package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.quipy.exceptions.TooManyRequestsException

@RestControllerAdvice
class GlobalExceptionHandler {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)
    }

    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(ex: TooManyRequestsException): ResponseEntity<Any> {
        logger.info("Too many requests, retry after {} ms", ex.retryAfterMillisecond)
        return ResponseEntity
            .status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, (System.currentTimeMillis() + ex.retryAfterMillisecond).toString())
            .build()
    }
}
