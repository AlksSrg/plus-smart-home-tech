package ru.yandex.practicum.exception.order;

/**
 * Исключение, выбрасываемое при попытке доступа к несуществующему заказу.
 */
public class OrderNotFoundException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public OrderNotFoundException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public OrderNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}