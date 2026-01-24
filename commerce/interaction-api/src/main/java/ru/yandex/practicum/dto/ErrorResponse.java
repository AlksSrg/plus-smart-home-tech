package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

/**
 * DTO для передачи информации об ошибке.
 * Используется для стандартизированного ответа при возникновении исключений.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    /**
     * Сообщение об ошибке для пользователя.
     */
    private String message;

    /**
     * Код ошибки для идентификации типа исключения.
     */
    private String errorCode;

    /**
     * HTTP статус ошибки.
     */
    private HttpStatus httpStatus;

    /**
     * Временная метка возникновения ошибки.
     */
    private long timestamp;

    /**
     * Создает объект ErrorResponse с автоматически установленной временной меткой.
     *
     * @param message    сообщение об ошибке
     * @param errorCode  код ошибки
     * @param httpStatus HTTP статус
     */
    public ErrorResponse(String message, String errorCode, HttpStatus httpStatus) {
        this.message = message;
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
        this.timestamp = System.currentTimeMillis();
    }
}