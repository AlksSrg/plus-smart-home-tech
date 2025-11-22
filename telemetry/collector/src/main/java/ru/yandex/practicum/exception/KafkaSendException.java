package ru.yandex.practicum.exception;

/**
 * Исключение при ошибке отправки сообщения в Kafka.
 */
public class KafkaSendException extends RuntimeException {

    public KafkaSendException(String message) {
        super(message);
    }
}