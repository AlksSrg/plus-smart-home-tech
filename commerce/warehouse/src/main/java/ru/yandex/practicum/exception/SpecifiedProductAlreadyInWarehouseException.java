package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;

public class SpecifiedProductAlreadyInWarehouseException extends BaseWarehouseException {

    public SpecifiedProductAlreadyInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }

    public SpecifiedProductAlreadyInWarehouseException(String message, Throwable cause) {
        super(message, cause, HttpStatus.BAD_REQUEST);
    }
}