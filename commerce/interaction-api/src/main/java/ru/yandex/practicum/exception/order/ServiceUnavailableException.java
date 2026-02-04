package ru.yandex.practicum.exception.order;

/**
 * Исключение, выбрасываемое при недоступности зависимого сервиса.
 * Используется для обработки ошибок в межсервисном взаимодействии.
 */
public class ServiceUnavailableException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public ServiceUnavailableException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message сообщение об ошибке
     * @param cause   причина исключения
     */
    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}