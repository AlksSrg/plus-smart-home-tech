package ru.yandex.practicum.exception;

/**
 * Исключение, выбрасываемое при ошибках отправки сообщений в Kafka.
 */
public class KafkaSendException extends RuntimeException {

    public KafkaSendException(String message) {
        super(message);
    }

    public KafkaSendException(String message, Throwable cause) {
        super(message, cause);
    }
}