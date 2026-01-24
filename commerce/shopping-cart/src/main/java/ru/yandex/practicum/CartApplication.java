package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * Главный класс приложения корзины покупок.
 * Запускает Spring Boot приложение с поддержкой микросервисной архитектуры.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients
public class CartApplication {

    /**
     * Точка входа в приложение.
     */
    public static void main(String[] args) {
        SpringApplication.run(CartApplication.class, args);
    }
}