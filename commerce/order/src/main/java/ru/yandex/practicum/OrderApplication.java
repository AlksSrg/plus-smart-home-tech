package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * Основной класс приложения Order Service.
 * Запускает микросервис для управления заказами в системе интернет-магазина.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "ru.yandex.practicum.feign")
public class OrderApplication {

    /**
     * Точка входа в приложение Order Service.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}