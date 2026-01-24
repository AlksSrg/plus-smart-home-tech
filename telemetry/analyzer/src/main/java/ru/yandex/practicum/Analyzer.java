package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Основной класс приложения Analyzer.
 * Запускает Spring Boot приложение для анализа данных телеметрии.
 * Обрабатывает данные из Kafka, выполняет аналитику и взаимодействует
 * с gRPC сервисами для маршрутизации команд.
 */
@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class Analyzer {

    /**
     * Точка входа в приложение.
     * Инициализирует Spring Boot приложение, запускает консьюмеры Kafka
     * и настраивает обработку входящих сообщений для анализа данных.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        log.info("Запуск сервиса Analyzer...");
        SpringApplication.run(Analyzer.class, args);
        log.info("Сервис Analyzer успешно запущен");
    }
}