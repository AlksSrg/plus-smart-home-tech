package ru.yandex.practicum.exception.order;

/**
 * Исключение, выбрасываемое при попытке доступа к несуществующему заказу.
 */
public class NoOrderFoundException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public NoOrderFoundException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public NoOrderFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}