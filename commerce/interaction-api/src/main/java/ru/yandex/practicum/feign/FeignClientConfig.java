package ru.yandex.practicum.feign;

import feign.Feign;
import feign.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.feign.decoder.FeignErrorDecoder;

/**
 * Конфигурационный класс для настройки Feign клиентов.
 * Определяет общие настройки для всех Feign клиентов.
 * Настройки включают обработку ошибок, логирование и таймауты.
 */
@Configuration
public class FeignClientConfig {

    /**
     * Создает кастомный билдер Feign с расширенным обработчиком ошибок.
     * Используется FeignErrorDecoder для улучшенной обработки HTTP ошибок
     * с извлечением сообщений из тела ответа и логированием.
     *
     * @return Feign.Builder с кастомным декодером ошибок
     */
    @Bean
    public Feign.Builder feignBuilder() {
        return Feign.builder()
                .errorDecoder(new FeignErrorDecoder());
    }

    /**
     * Настраивает уровень логирования Feign клиентов.
     * Уровень FULL включает логирование заголовков, тела запросов и ответов,
     * что полезно для отладки межсервисного взаимодействия.
     *
     * @return уровень логирования FULL
     */
    @Bean
    public Logger.Level feignLoggerLevel() {
        return Logger.Level.FULL;
    }
}