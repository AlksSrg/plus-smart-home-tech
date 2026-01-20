package ru.yandex.practicum.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class WarehouseExceptionHandler {

    @ExceptionHandler(SpecifiedProductAlreadyInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleProductAlreadyExists(
            SpecifiedProductAlreadyInWarehouseException ex) {
        log.error("Product already exists in warehouse: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    @ExceptionHandler(NoSpecifiedProductInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleProductNotFound(
            NoSpecifiedProductInWarehouseException ex) {
        log.error("Product not found in warehouse: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    @ExceptionHandler(ProductInShoppingCartLowQuantityInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleInsufficientQuantity(
            ProductInShoppingCartLowQuantityInWarehouseException ex) {
        log.error("Insufficient quantity in warehouse: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());
        response.put("unavailableProducts", ex.getUnavailableProducts());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", HttpStatus.INTERNAL_SERVER_ERROR);
        response.put("userMessage", "Внутренняя ошибка сервера");
        response.put("message", ex.getMessage());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}