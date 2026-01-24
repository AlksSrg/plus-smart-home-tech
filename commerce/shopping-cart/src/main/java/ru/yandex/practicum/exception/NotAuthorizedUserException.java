package ru.yandex.practicum.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Исключение, выбрасываемое при отсутствии авторизации пользователя.
 * Автоматически возвращает HTTP статус 401 UNAUTHORIZED.
 */
@ResponseStatus(HttpStatus.UNAUTHORIZED)
public class NotAuthorizedUserException extends RuntimeException {
    /**
     * Создает исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public NotAuthorizedUserException(String message) {
        super(message);
    }
}