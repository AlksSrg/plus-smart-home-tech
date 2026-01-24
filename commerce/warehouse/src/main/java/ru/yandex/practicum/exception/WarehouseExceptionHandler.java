package ru.yandex.practicum.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * Глобальный обработчик исключений для складского модуля.
 * Обрабатывает все исключения, связанные с операциями на складе.
 */
@Slf4j
@RestControllerAdvice
public class WarehouseExceptionHandler {

    /**
     * Обрабатывает исключение при попытке добавить существующий товар на склад.
     *
     * @param ex исключение SpecifiedProductAlreadyInWarehouseException
     * @return ResponseEntity с информацией об ошибке
     */
    @ExceptionHandler(SpecifiedProductAlreadyInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleProductAlreadyExists(
            SpecifiedProductAlreadyInWarehouseException ex) {
        log.error("Товар уже существует на складе: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    /**
     * Обрабатывает исключение при отсутствии товара на складе.
     *
     * @param ex исключение NoSpecifiedProductInWarehouseException
     * @return ResponseEntity с информацией об ошибке
     */
    @ExceptionHandler(NoSpecifiedProductInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleProductNotFound(
            NoSpecifiedProductInWarehouseException ex) {
        log.error("Товар не найден на складе: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    /**
     * Обрабатывает исключение при недостаточном количестве товара на складе.
     *
     * @param ex исключение ProductInShoppingCartLowQuantityInWarehouseException
     * @return ResponseEntity с информацией об ошибке и списком недостающих товаров
     */
    @ExceptionHandler(ProductInShoppingCartLowQuantityInWarehouseException.class)
    public ResponseEntity<Map<String, Object>> handleInsufficientQuantity(
            ProductInShoppingCartLowQuantityInWarehouseException ex) {
        log.error("Недостаточное количество товара на складе: {}", ex.getMessage());

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", ex.getHttpStatus());
        response.put("userMessage", ex.getMessage());
        response.put("message", ex.getMessage());
        response.put("unavailableProducts", ex.getUnavailableProducts());

        return ResponseEntity.status(ex.getHttpStatus()).body(response);
    }

    /**
     * Обрабатывает все необработанные исключения.
     *
     * @param ex исключение
     * @return ResponseEntity с информацией о внутренней ошибке сервера
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(Exception ex) {
        log.error("Непредвиденная ошибка: {}", ex.getMessage(), ex);

        Map<String, Object> response = new HashMap<>();
        response.put("httpStatus", HttpStatus.INTERNAL_SERVER_ERROR);
        response.put("userMessage", "Внутренняя ошибка сервера");
        response.put("message", ex.getMessage());

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}