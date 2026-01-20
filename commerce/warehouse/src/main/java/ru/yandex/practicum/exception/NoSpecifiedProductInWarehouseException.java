package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class NoSpecifiedProductInWarehouseException extends BaseWarehouseException {

    public NoSpecifiedProductInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }

    public NoSpecifiedProductInWarehouseException(String message, Throwable cause) {
        super(message, cause, HttpStatus.BAD_REQUEST);
    }
}