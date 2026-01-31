package ru.yandex.practicum.exception.warehouse;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Базовый класс исключений для складской системы.
 * Содержит HTTP-статус для возврата клиенту.
 */
@Getter
public abstract class BaseWarehouseException extends RuntimeException {

    private final HttpStatus httpStatus;

    /**
     * Конструктор исключения с сообщением и HTTP-статусом.
     *
     * @param message    сообщение об ошибке
     * @param httpStatus HTTP-статус для ответа
     */
    public BaseWarehouseException(String message, HttpStatus httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    /**
     * Конструктор исключения с сообщением, причиной и HTTP-статусом.
     *
     * @param message    сообщение об ошибке
     * @param cause      причина исключения
     * @param httpStatus HTTP-статус для ответа
     */
    public BaseWarehouseException(String message, Throwable cause, HttpStatus httpStatus) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }
}