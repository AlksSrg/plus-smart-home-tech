package ru.yandex.practicum.exception;

/**
 * Исключение, выбрасываемое при попытке выполнить операцию с деактивированной корзиной.
 */
public class CartDeactivateException extends RuntimeException {
    /**
     * Создает исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public CartDeactivateException(String message) {
        super(message);
    }
}