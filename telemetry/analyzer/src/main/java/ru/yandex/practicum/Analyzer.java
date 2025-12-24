package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class Analyzer {

    public static void main(String[] args) {
        log.info("Запуск сервиса Analyzer...");
        SpringApplication.run(Analyzer.class, args);
        log.info("Сервис Analyzer успешно запущен");
    }
}