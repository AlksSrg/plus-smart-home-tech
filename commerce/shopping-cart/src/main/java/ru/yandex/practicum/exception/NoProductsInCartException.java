package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Исключение, выбрасываемое при отсутствии товаров в корзине.
 * Автоматически возвращает HTTP статус 400 BAD_REQUEST.
 */
@ResponseStatus(HttpStatus.BAD_REQUEST)
public class NoProductsInCartException extends RuntimeException {
    /**
     * Создает исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public NoProductsInCartException(String message) {
        super(message);
    }
}