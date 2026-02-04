package ru.yandex.practicum.feign.decoder;

import feign.Response;
import feign.codec.ErrorDecoder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Кастомный декодер ошибок для Feign клиентов.
 * Преобразует HTTP ошибки в соответствующие исключения.
 */
@Slf4j
public class FeignErrorDecoder implements ErrorDecoder {

    private final ErrorDecoder defaultDecoder = new Default();

    @Override
    public Exception decode(String methodKey, Response response) {
        String errorMessage = extractErrorMessage(response);

        return switch (response.status()) {
            case 404 -> {
                log.error("Ресурс не найден: {} - {}", response.request().url(), errorMessage);
                yield new RuntimeException("Ресурс не найден: " + errorMessage);
            }
            case 400 -> {
                log.error("Некорректный запрос: {} - {}", response.request().url(), errorMessage);
                yield new RuntimeException("Некорректный запрос: " + errorMessage);
            }
            case 409 -> {
                log.error("Конфликт состояния: {} - {}", response.request().url(), errorMessage);
                yield new RuntimeException("Конфликт состояния: " + errorMessage);
            }
            case 503, 504 -> {
                log.error("Сервис недоступен: {} - {}", response.request().url(), errorMessage);
                yield new RuntimeException("Сервис недоступен: " + errorMessage);
            }
            default -> defaultDecoder.decode(methodKey, response);
        };
    }

    /**
     * Извлекает сообщение об ошибке из тела ответа.
     *
     * @param response HTTP ответ
     * @return текст сообщения об ошибке
     */
    private String extractErrorMessage(Response response) {
        try {
            if (response.body() != null) {
                return new String(response.body().asInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            log.warn("Не удалось прочитать тело ответа", e);
        }
        return "HTTP статус: " + response.status();
    }
}