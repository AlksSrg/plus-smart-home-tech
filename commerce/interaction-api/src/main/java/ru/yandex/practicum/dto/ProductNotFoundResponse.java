package ru.yandex.practicum.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

/**
 * DTO для ответа при отсутствии товара.
 * Используется для стандартизированного сообщения о ненайденном товаре.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductNotFoundResponse {
    /**
     * HTTP статус ответа.
     */
    private HttpStatus httpStatus;

    /**
     * Сообщение для пользователя.
     */
    private String userMessage;

    /**
     * Техническое сообщение об ошибке.
     */
    private String message;
}