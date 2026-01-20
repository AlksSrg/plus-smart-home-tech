package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.dto.ProductNotFoundResponse;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<ProductNotFoundResponse> handleProductNotFoundException(
            ProductNotFoundException ex, WebRequest request) {

        ProductNotFoundResponse error = new ProductNotFoundResponse();
        error.setHttpStatus(HttpStatus.NOT_FOUND);
        error.setMessage(ex.getMessage());
        error.setUserMessage("Товар не найден");

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<ProductNotFoundResponse> handleRuntimeException(
            RuntimeException ex, WebRequest request) {

        ProductNotFoundResponse error = new ProductNotFoundResponse();
        error.setHttpStatus(HttpStatus.BAD_REQUEST);
        error.setMessage(ex.getMessage());
        error.setUserMessage("Ошибка при выполнении операции");

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProductNotFoundResponse> handleGenericException(
            Exception ex, WebRequest request) {

        ProductNotFoundResponse error = new ProductNotFoundResponse();
        error.setHttpStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        error.setMessage(ex.getMessage());
        error.setUserMessage("Внутренняя ошибка сервера");

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }
}