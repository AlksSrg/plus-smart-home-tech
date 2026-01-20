package ru.yandex.practicum.exception;

public class CartDeactivateException extends RuntimeException {
    public CartDeactivateException(String message) {
        super(message);
    }
}