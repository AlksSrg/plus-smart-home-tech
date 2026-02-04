package ru.yandex.practicum.exception.payment;

/**
 * Исключение, выбрасываемое когда платеж не найден.
 * Возникает при попытке получить несуществующий платеж.
 */
public class NoFoundPaymentException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public NoFoundPaymentException(String message) {
        super(message);
    }
}