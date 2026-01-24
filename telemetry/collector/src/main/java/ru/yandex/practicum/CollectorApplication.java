package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Основной класс приложения Collector.
 * Запускает Spring Boot приложение для сбора данных телеметрии через gRPC.
 * Слушает gRPC запросы на порту 59091 и отправляет данные в Kafka.
 */
@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class CollectorApplication {
    /**
     * Точка входа в приложение Collector.
     * Инициализирует Spring Boot приложение и запускает gRPC сервер.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        log.info("Запуск Collector gRPC сервиса...");
        SpringApplication.run(CollectorApplication.class, args);
        log.info("Collector gRPC сервис успешно запущен на порту 59091");
    }
}