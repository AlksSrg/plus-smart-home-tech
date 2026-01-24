package ru.yandex.practicum.feign;

import feign.Logger;
import feign.RequestInterceptor;
import feign.codec.ErrorDecoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Конфигурация для Feign-клиентов.
 * Определяет настройки логирования, обработки ошибок и перехватчики запросов.
 */
@Configuration
public class FeignClientConfig {

    /**
     * Настраивает уровень логирования для Feign-клиентов.
     *
     * @return уровень логирования FULL (полный)
     */
    @Bean
    public Logger.Level feignLoggerLevel() {
        return Logger.Level.FULL;
    }

    /**
     * Регистрирует декодер ошибок для обработки исключений Feign.
     *
     * @return кастомный декодер ошибок
     */
    @Bean
    public ErrorDecoder errorDecoder() {
        return new FeignErrorDecoder();
    }

    /**
     * Регистрирует перехватчик запросов для добавления заголовков.
     *
     * @return перехватчик, добавляющий заголовки Content-Type и Accept
     */
    @Bean
    public RequestInterceptor requestInterceptor() {
        return requestTemplate -> {
            requestTemplate.header("Content-Type", "application/json");
            requestTemplate.header("Accept", "application/json");
        };
    }
}