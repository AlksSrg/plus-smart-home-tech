package ru.yandex.practicum.gateway.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Глобальный обработчик исключений для Spring WebFlux Gateway.
 * Преобразует исключения в структурированные JSON-ответы.
 */
@Component
@Order(-2)
public class GatewayGlobalExceptionHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GatewayGlobalExceptionHandler.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Обрабатывает исключение и возвращает JSON-ответ.
     */
    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        log.error("Exception occurred during request to {}: {}",
                exchange.getRequest().getPath(), ex.getMessage(), ex);

        ServerHttpResponse response = exchange.getResponse();
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        HttpStatus status = determineHttpStatus(ex);
        response.setStatusCode(status);

        Map<String, Object> errorResponse = buildErrorResponse(ex, status, exchange);

        return response.writeWith(Mono.fromSupplier(() -> {
            try {
                byte[] bytes = objectMapper.writeValueAsBytes(errorResponse);
                DataBufferFactory bufferFactory = response.bufferFactory();
                return bufferFactory.wrap(bytes);
            } catch (JsonProcessingException e) {
                log.error("Error serializing error response", e);
                return response.bufferFactory().wrap("{\"error\":\"Internal server error\"}".getBytes(StandardCharsets.UTF_8));
            }
        }));
    }

    /**
     * Определяет HTTP-статус на основе типа исключения.
     */
    private HttpStatus determineHttpStatus(Throwable ex) {
        // Обработка всех кастомных исключений из interaction-api
        if (ex instanceof ru.yandex.practicum.exception.delivery.DeliveryNotFoundException ||
                ex instanceof ru.yandex.practicum.exception.order.NoOrderFoundException ||
                ex instanceof ru.yandex.practicum.exception.order.OrderNotFoundException ||
                ex instanceof ru.yandex.practicum.exception.payment.NoFoundPaymentException ||
                ex instanceof ru.yandex.practicum.exception.shoppingStore.ProductNotFoundException) {
            return HttpStatus.NOT_FOUND;
        } else if (ex instanceof ru.yandex.practicum.exception.order.ServiceUnavailableException) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        } else if (ex instanceof ru.yandex.practicum.exception.payment.ImpossibleCalculateCostOrderException ||
                ex instanceof ru.yandex.practicum.exception.shoppingCart.CartDeactivateException ||
                ex instanceof ru.yandex.practicum.exception.shoppingCart.NoProductsInCartException ||
                ex instanceof ru.yandex.practicum.exception.warehouse.BaseWarehouseException ||
                ex instanceof IllegalArgumentException) {
            return HttpStatus.BAD_REQUEST;
        } else if (ex instanceof ru.yandex.practicum.exception.shoppingCart.NotAuthorizedUserException) {
            return HttpStatus.UNAUTHORIZED;
        } else if (ex instanceof IllegalStateException) {
            return HttpStatus.CONFLICT;
        } else if (ex instanceof ResponseStatusException) {
            // Фикс для ResponseStatusException
            ResponseStatusException rse = (ResponseStatusException) ex;
            return HttpStatus.valueOf(rse.getStatusCode().value());
        } else {
            return HttpStatus.INTERNAL_SERVER_ERROR;
        }
    }

    /**
     * Создает структурированный объект ответа с ошибкой.
     */
    private Map<String, Object> buildErrorResponse(Throwable ex, HttpStatus status, ServerWebExchange exchange) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", System.currentTimeMillis());
        errorResponse.put("path", exchange.getRequest().getPath().value());
        errorResponse.put("status", status.value());
        errorResponse.put("error", status.getReasonPhrase());
        errorResponse.put("message", getErrorMessage(ex));
        errorResponse.put("code", getErrorCode(ex));

        return errorResponse;
    }

    /**
     * Извлекает сообщение об ошибке из исключения.
     */
    private String getErrorMessage(Throwable ex) {
        if (ex instanceof ResponseStatusException) {
            String reason = ((ResponseStatusException) ex).getReason();
            return reason != null ? reason : ex.getMessage();
        }
        return ex.getMessage() != null ? ex.getMessage() : "Internal server error";
    }

    /**
     * Возвращает кодовое обозначение ошибки для различных типов исключений.
     */
    private String getErrorCode(Throwable ex) {
        if (ex instanceof ru.yandex.practicum.exception.delivery.DeliveryNotFoundException) {
            return "DELIVERY_NOT_FOUND";
        } else if (ex instanceof ru.yandex.practicum.exception.order.NoOrderFoundException ||
                ex instanceof ru.yandex.practicum.exception.order.OrderNotFoundException) {
            return "ORDER_NOT_FOUND";
        } else if (ex instanceof ru.yandex.practicum.exception.order.ServiceUnavailableException) {
            return "SERVICE_UNAVAILABLE";
        } else if (ex instanceof ru.yandex.practicum.exception.payment.ImpossibleCalculateCostOrderException) {
            return "CALCULATION_ERROR";
        } else if (ex instanceof ru.yandex.practicum.exception.payment.NoFoundPaymentException) {
            return "PAYMENT_NOT_FOUND";
        } else if (ex instanceof ru.yandex.practicum.exception.shoppingCart.CartDeactivateException) {
            return "CART_DEACTIVATED";
        } else if (ex instanceof ru.yandex.practicum.exception.shoppingCart.NoProductsInCartException) {
            return "EMPTY_CART";
        } else if (ex instanceof ru.yandex.practicum.exception.shoppingCart.NotAuthorizedUserException) {
            return "UNAUTHORIZED";
        } else if (ex instanceof ru.yandex.practicum.exception.shoppingStore.ProductNotFoundException) {
            return "PRODUCT_NOT_FOUND";
        } else if (ex instanceof IllegalArgumentException) {
            return "INVALID_ARGUMENT";
        } else if (ex instanceof IllegalStateException) {
            return "ILLEGAL_STATE";
        }
        return "INTERNAL_SERVER_ERROR";
    }
}