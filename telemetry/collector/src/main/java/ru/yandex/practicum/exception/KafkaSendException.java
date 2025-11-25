package ru.yandex.practicum.exception;

/**
 * Исключение, выбрасываемое при ошибках отправки сообщений в Kafka.
 */
public class KafkaSendException extends RuntimeException {

    /**
     * Конструктор с сообщением об ошибке.
     *
     * @param message сообщение об ошибке
     */
    public KafkaSendException(String message) {
        super(message);
    }

    /**
     * Конструктор с сообщением об ошибке и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public KafkaSendException(String message, Throwable cause) {
        super(message, cause);
    }
}