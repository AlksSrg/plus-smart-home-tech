package ru.yandex.practicum.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.http.HttpStatus;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import ru.yandex.practicum.dto.product.ErrorResponse;
import ru.yandex.practicum.exception.delivery.DeliveryNotFoundException;
import ru.yandex.practicum.exception.order.NoOrderFoundException;
import ru.yandex.practicum.exception.order.OrderNotFoundException;
import ru.yandex.practicum.exception.order.ServiceUnavailableException;
import ru.yandex.practicum.exception.payment.ImpossibleCalculateCostOrderException;
import ru.yandex.practicum.exception.payment.NoFoundPaymentException;
import ru.yandex.practicum.exception.shoppingCart.CartDeactivateException;
import ru.yandex.practicum.exception.shoppingCart.NoProductsInCartException;
import ru.yandex.practicum.exception.shoppingCart.NotAuthorizedUserException;
import ru.yandex.practicum.exception.shoppingStore.ProductNotFoundException;
import ru.yandex.practicum.exception.warehouse.BaseWarehouseException;
import ru.yandex.practicum.exception.warehouse.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.warehouse.ProductInShoppingCartLowQuantityInWarehouseException;
import ru.yandex.practicum.exception.warehouse.SpecifiedProductAlreadyInWarehouseException;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Глобальный обработчик исключений для Spring MVC приложений.
 * Используется в сервисах: order, shopping-cart, delivery, payment и других.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // =========================== Delivery Exceptions ===========================

    /**
     * Обрабатывает исключение при отсутствии доставки.
     *
     * @param e исключение DeliveryNotFoundException
     * @return ответ с кодом 404 и сообщением об ошибке
     */
    @ExceptionHandler(DeliveryNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleDeliveryNotFoundException(DeliveryNotFoundException e) {
        log.error("Доставка не найдена: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    // =========================== Order Exceptions ===========================

    /**
     * Обрабатывает исключение при отсутствии заказа.
     *
     * @param e исключение NoOrderFoundException или OrderNotFoundException
     * @return ответ с кодом 404 и сообщением об ошибке
     */
    @ExceptionHandler({NoOrderFoundException.class, OrderNotFoundException.class})
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleOrderNotFoundException(RuntimeException e) {
        log.error("Заказ не найден: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключение при недоступности сервиса.
     *
     * @param e исключение ServiceUnavailableException
     * @return ответ с кодом 503 и сообщением об ошибке
     */
    @ExceptionHandler(ServiceUnavailableException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> handleServiceUnavailableException(ServiceUnavailableException e) {
        log.error("Сервис недоступен: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    // =========================== Payment Exceptions ===========================

    /**
     * Обрабатывает исключение при невозможности расчета стоимости заказа.
     *
     * @param e исключение ImpossibleCalculateCostOrderException
     * @return ответ с кодом 400 и сообщением об ошибке
     */
    @ExceptionHandler(ImpossibleCalculateCostOrderException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleImpossibleCalculateCostOrderException(ImpossibleCalculateCostOrderException e) {
        log.error("Невозможно рассчитать стоимость заказа: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключение при отсутствии платежа.
     *
     * @param e исключение NoFoundPaymentException
     * @return ответ с кодом 404 и сообщением об ошибке
     */
    @ExceptionHandler(NoFoundPaymentException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleNoFoundPaymentException(NoFoundPaymentException e) {
        log.error("Платеж не найден: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    // =========================== Shopping Cart Exceptions ===========================

    /**
     * Обрабатывает исключение при операции с деактивированной корзиной.
     *
     * @param e исключение CartDeactivateException
     * @return ответ с кодом 400 и сообщением об ошибке
     */
    @ExceptionHandler(CartDeactivateException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleCartDeactivateException(CartDeactivateException e) {
        log.error("Корзина деактивирована: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключение при отсутствии товаров в корзине.
     *
     * @param e исключение NoProductsInCartException
     * @return ответ с кодом 400 и сообщением об ошибке
     */
    @ExceptionHandler(NoProductsInCartException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleNoProductsInCartException(NoProductsInCartException e) {
        log.error("Нет товаров в корзине: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключение при отсутствии авторизации пользователя.
     *
     * @param e исключение NotAuthorizedUserException
     * @return ответ с кодом 401 и сообщением об ошибке
     */
    @ExceptionHandler(NotAuthorizedUserException.class)
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public Map<String, String> handleNotAuthorizedUserException(NotAuthorizedUserException e) {
        log.error("Пользователь не авторизован: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    // =========================== Shopping Store Exceptions ===========================

    /**
     * Обрабатывает исключение при отсутствии товара в магазине.
     *
     * @param ex исключение ProductNotFoundException
     * @return ответ с кодом 404 и структурированным сообщением об ошибке
     */
    @ExceptionHandler(ru.yandex.practicum.exception.shoppingStore.ProductNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleProductNotFoundException(ProductNotFoundException ex) {
        log.warn("Товар не найден: {}", ex.getMessage());
        return new ErrorResponse(
                ex.getMessage(),
                "PRODUCT_NOT_FOUND",
                HttpStatus.NOT_FOUND
        );
    }

    // =========================== Warehouse Exceptions ===========================

    /**
     * Обрабатывает исключение при попытке добавить существующий товар на склад.
     *
     * @param ex исключение SpecifiedProductAlreadyInWarehouseException
     * @return ответ с кодом 400 и информацией об ошибке
     */
    @ExceptionHandler(SpecifiedProductAlreadyInWarehouseException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleProductAlreadyExists(SpecifiedProductAlreadyInWarehouseException ex) {
        log.error("Товар уже существует на складе: {}", ex.getMessage());
        return Map.of(
                "error", ex.getMessage(),
                "status", ex.getHttpStatus().value(),
                "message", ex.getMessage()
        );
    }

    /**
     * Обрабатывает исключение при отсутствии товара на складе.
     *
     * @param ex исключение NoSpecifiedProductInWarehouseException
     * @return ответ с кодом 400 и информацией об ошибке
     */
    @ExceptionHandler(NoSpecifiedProductInWarehouseException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleProductNotFound(NoSpecifiedProductInWarehouseException ex) {
        log.error("Товар не найден на складе: {}", ex.getMessage());
        return Map.of(
                "error", ex.getMessage(),
                "status", ex.getHttpStatus().value(),
                "message", ex.getMessage()
        );
    }

    /**
     * Обрабатывает исключение при недостаточном количестве товара на складе.
     *
     * @param ex исключение ProductInShoppingCartLowQuantityInWarehouseException
     * @return ответ с кодом 400 и информацией об ошибке с списком недостающих товаров
     */
    @ExceptionHandler(ProductInShoppingCartLowQuantityInWarehouseException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> handleInsufficientQuantity(ProductInShoppingCartLowQuantityInWarehouseException ex) {
        log.error("Недостаточное количество товара на складе: {}", ex.getMessage());
        return Map.of(
                "error", ex.getMessage(),
                "status", ex.getHttpStatus().value(),
                "message", ex.getMessage(),
                "unavailableProducts", ex.getUnavailableProducts()
        );
    }

    // =========================== Common Exceptions ===========================

    /**
     * Обрабатывает исключение при некорректных входных данных.
     *
     * @param e исключение IllegalArgumentException
     * @return ответ с кодом 400 и сообщением об ошибке
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Object handleIllegalArgumentException(IllegalArgumentException e) {
        log.error("Некорректные данные: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключение при некорректном состоянии системы.
     *
     * @param e исключение IllegalStateException
     * @return ответ с кодом 409 и сообщением об ошибке
     */
    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> handleIllegalStateException(IllegalStateException e) {
        log.error("Некорректное состояние: {}", e.getMessage());
        return Map.of("error", e.getMessage());
    }

    /**
     * Обрабатывает исключения валидации @Valid.
     *
     * @param ex исключение MethodArgumentNotValidException
     * @return ответ с кодом 400 и структурированным сообщением об ошибке
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleValidationExceptions(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining(", "));

        log.warn("Ошибка валидации: {}", message);

        return new ErrorResponse(
                message,
                "VALIDATION_ERROR",
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Обрабатывает исключения типа MethodArgumentTypeMismatchException.
     * Например, когда UUID передается в неправильном формате.
     *
     * @param ex исключение MethodArgumentTypeMismatchException
     * @return ответ с кодом 400 и структурированным сообщением об ошибке
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMethodArgumentTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String message;

        if (ex.getRequiredType() != null && ex.getRequiredType().equals(UUID.class)) {
            message = "Invalid UUID format: '" + ex.getValue() + "'. UUID must be in format: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'";
        } else if (ex.getRequiredType() != null) {
            message = "Invalid value '" + ex.getValue() + "' for parameter '" + ex.getName() +
                    "'. Expected type: " + ex.getRequiredType().getSimpleName();
        } else {
            message = "Invalid value for parameter '" + ex.getName() + "'";
        }

        log.warn("Неверный формат параметра: {}", message);

        return new ErrorResponse(
                message,
                "INVALID_PARAMETER",
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Обрабатывает исключения неподдерживаемых HTTP методов.
     *
     * @param ex исключение HttpRequestMethodNotSupportedException
     * @return ответ с кодом 405 и структурированным сообщением об ошибке
     */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    public ErrorResponse handleHttpRequestMethodNotSupported(HttpRequestMethodNotSupportedException ex) {
        log.warn("Метод не поддерживается: {}", ex.getMessage());

        return new ErrorResponse(
                "HTTP method '" + ex.getMethod() + "' is not supported for this endpoint",
                "METHOD_NOT_ALLOWED",
                HttpStatus.METHOD_NOT_ALLOWED
        );
    }

    /**
     * Обрабатывает исключение при неправильных параметрах сортировки.
     *
     * @param ex исключение PropertyReferenceException
     * @return ответ с кодом 400 и структурированным сообщением об ошибке
     */
    @ExceptionHandler(PropertyReferenceException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handlePropertyReferenceException(PropertyReferenceException ex) {
        log.warn("Ошибка в параметрах сортировки: {}", ex.getMessage());

        String message = "Invalid sort property. " + ex.getMessage() +
                ". Supported properties: productName, price, productCategory, productState, quantityState, createdAt, updatedAt";

        return new ErrorResponse(
                message,
                "INVALID_SORT_PARAMETER",
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Обрабатывает исключение BaseWarehouseException и его наследников.
     *
     * @param ex исключение BaseWarehouseException
     * @return ответ с соответствующим HTTP статусом и информацией об ошибке
     */
    @ExceptionHandler(BaseWarehouseException.class)
    public Map<String, Object> handleBaseWarehouseException(BaseWarehouseException ex) {
        log.error("Ошибка склада: {}", ex.getMessage());
        return Map.of(
                "error", ex.getMessage(),
                "status", ex.getHttpStatus().value(),
                "message", ex.getMessage()
        );
    }

    /**
     * Обрабатывает все необработанные исключения.
     *
     * @param ex исключение Exception
     * @return ответ с кодом 500 и общим сообщением об ошибке
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleException(Exception ex) {
        log.error("Внутренняя ошибка сервера: {}", ex.getMessage(), ex);
        return Map.of("error", "Внутренняя ошибка сервера: " + ex.getMessage());
    }
}