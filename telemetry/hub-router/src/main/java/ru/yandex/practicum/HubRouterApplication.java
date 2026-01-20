package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * Основной класс приложения Hub Router.
 * Запускает gRPC сервер для маршрутизации команд к хабам устройств.
 * Обрабатывает команды управления и передает их соответствующим устройствам.
 */
@SpringBootApplication
@EnableDiscoveryClient
public class HubRouterApplication {

    /**
     * Точка входа в приложение Hub Router.
     * Инициализирует Spring Boot приложение и запускает gRPC сервер.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        SpringApplication.run(HubRouterApplication.class, args);
    }
}