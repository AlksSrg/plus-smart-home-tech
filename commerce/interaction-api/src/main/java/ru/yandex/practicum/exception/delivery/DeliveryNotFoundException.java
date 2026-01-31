package ru.yandex.practicum.exception.delivery;

/**
 * Исключение, выбрасываемое когда доставка не найдена в системе.
 */
public class DeliveryNotFoundException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public DeliveryNotFoundException(String message) {
        super(message);
    }
}