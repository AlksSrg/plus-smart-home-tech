package ru.yandex.practicum.kafka.telemetry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.kafka.telemetry.starter.AggregationStarter;

/**
 * Основной класс приложения Aggregator.
 * Запускает Spring Boot приложение для агрегации данных с датчиков.
 * Потребляет данные из Kafka, агрегирует их и сохраняет в базу данных.
 */
@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableDiscoveryClient
public class Aggregator {

    /**
     * Точка входа в приложение.
     * Инициализирует Spring Boot контекст, запускает агрегацию данных
     * и регистрирует обработчик завершения работы приложения.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        log.info("Запуск сервиса Aggregator...");

        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);

        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        context.registerShutdownHook();
        aggregator.start();

        log.info("Сервис Aggregator успешно запущен");
    }
}