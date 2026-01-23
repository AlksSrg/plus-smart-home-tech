package ru.yandex.practicum.feign;

import feign.Response;
import feign.codec.ErrorDecoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Кастомный декодер ошибок для Feign-клиентов.
 * Преобразует HTTP-ответы в ResponseStatusException для единообразной обработки ошибок.
 */
@Slf4j
public class FeignErrorDecoder implements ErrorDecoder {

    private final ErrorDecoder defaultErrorDecoder = new Default();

    /**
     * Декодирует HTTP-ответ в исключение.
     *
     * @param methodKey ключ метода Feign
     * @param response  HTTP-ответ от сервиса
     * @return исключение для выбранного HTTP-статуса
     */
    @Override
    public Exception decode(String methodKey, Response response) {
        HttpStatus httpStatus = HttpStatus.valueOf(response.status());

        switch (httpStatus) {
            case NOT_FOUND:
                return new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Ресурс не найден: " + extractMessageFromResponse(response));
            case BAD_REQUEST:
                return new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "Некорректный запрос: " + extractMessageFromResponse(response));
            case UNAUTHORIZED:
                return new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                        "Требуется авторизация");
            case FORBIDDEN:
                return new ResponseStatusException(HttpStatus.FORBIDDEN,
                        "Доступ запрещен");
            case SERVICE_UNAVAILABLE:
                return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Сервис временно недоступен: " + extractMessageFromResponse(response));
            default:
                if (response.status() == 503) {
                    return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                            "Сервис временно недоступен: " + extractMessageFromResponse(response));
                }
                return defaultErrorDecoder.decode(methodKey, response);
        }
    }

    /**
     * Извлекает сообщение из тела HTTP-ответа.
     *
     * @param response HTTP-ответ
     * @return текст сообщения или код ошибки
     */
    private String extractMessageFromResponse(Response response) {
        try {
            if (response.body() != null) {
                return new String(response.body().asInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            log.warn("Не удалось прочитать тело ответа Feign", e);
        }
        return "Код ошибки: " + response.status();
    }
}