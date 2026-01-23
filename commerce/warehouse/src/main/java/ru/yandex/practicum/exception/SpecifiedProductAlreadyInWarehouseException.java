package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

/**
 * Исключение, выбрасываемое при попытке добавить товар,
 * который уже существует на складе.
 */
public class SpecifiedProductAlreadyInWarehouseException extends BaseWarehouseException {

    /**
     * Конструктор исключения с сообщением.
     *
     * @param message сообщение об ошибке
     */
    public SpecifiedProductAlreadyInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }

    /**
     * Конструктор исключения с сообщением и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public SpecifiedProductAlreadyInWarehouseException(String message, Throwable cause) {
        super(message, cause, HttpStatus.BAD_REQUEST);
    }
}