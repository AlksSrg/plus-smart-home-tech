package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

/**
 * Исключение, выбрасываемое при попытке получить информацию о товаре,
 * который отсутствует на складе.
 */
public class NoSpecifiedProductInWarehouseException extends BaseWarehouseException {

    /**
     * Конструктор исключения с сообщением.
     *
     * @param message сообщение об ошибке
     */
    public NoSpecifiedProductInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }

    /**
     * Конструктор исключения с сообщением и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public NoSpecifiedProductInWarehouseException(String message, Throwable cause) {
        super(message, cause, HttpStatus.BAD_REQUEST);
    }
}