package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class CollectorApplication {
    public static void main(String[] args) {
        log.info("Запуск Collector gRPC сервиса...");
        SpringApplication.run(CollectorApplication.class, args);
        log.info("Collector gRPC сервис успешно запущен на порту 59091");
    }
}