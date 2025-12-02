package ru.yandex.practicum.kafka.telemetry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Основной класс приложения Aggregator.
 * Запускает Spring Boot приложение для агрегации данных с датчиков.
 */
@Slf4j
@SpringBootApplication
public class Aggregator {

    /**
     * Точка входа в приложение.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        log.info("Запуск сервиса Aggregator...");
        SpringApplication.run(Aggregator.class, args);
        log.info("Сервис Aggregator успешно запущен");
    }
}