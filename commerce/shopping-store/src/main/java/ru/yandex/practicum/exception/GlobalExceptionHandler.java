package ru.yandex.practicum.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import ru.yandex.practicum.dto.ErrorResponse;

import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Глобальный обработчик исключений для всего приложения.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Обрабатывает исключение ProductNotFoundException.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
     */
    @ExceptionHandler(ProductNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleProductNotFoundException(ProductNotFoundException ex) {
        log.warn("Товар не найден: {}", ex.getMessage());

        return new ErrorResponse(
                ex.getMessage(),
                "PRODUCT_NOT_FOUND",
                HttpStatus.NOT_FOUND
        );
    }

    /**
     * Обрабатывает исключение IllegalArgumentException.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleIllegalArgumentException(IllegalArgumentException ex) {
        log.warn("Некорректные данные: {}", ex.getMessage());

        return new ErrorResponse(
                ex.getMessage(),
                "VALIDATION_ERROR",
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Обрабатывает исключения валидации @Valid.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
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
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
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
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
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
     * Обрабатывает исключение org.springframework.data.mapping.PropertyReferenceException.
     * Возникает при неправильных параметрах сортировки.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
     */
    @ExceptionHandler(org.springframework.data.mapping.PropertyReferenceException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handlePropertyReferenceException(org.springframework.data.mapping.PropertyReferenceException ex) {
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
     * Обрабатывает все остальные исключения RuntimeException.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
     */
    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleRuntimeException(RuntimeException ex) {
        log.error("Ошибка при выполнении операции: {}", ex.getMessage(), ex);

        return new ErrorResponse(
                ex.getMessage(),
                "RUNTIME_ERROR",
                HttpStatus.BAD_REQUEST
        );
    }

    /**
     * Обрабатывает все остальные исключения.
     *
     * @param ex Исключение
     * @return Ответ с сообщением об ошибке
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleGenericException(Exception ex) {
        log.error("Внутренняя ошибка сервера: {}", ex.getMessage(), ex);

        return new ErrorResponse(
                "Internal server error",
                "INTERNAL_SERVER_ERROR",
                HttpStatus.INTERNAL_SERVER_ERROR
        );
    }
}