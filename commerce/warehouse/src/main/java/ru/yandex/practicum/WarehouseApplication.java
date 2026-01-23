package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * Главный класс приложения склада.
 * Микросервис для управления складскими операциями и товарами.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "ru.yandex.practicum.feign")
@ConfigurationPropertiesScan({"ru.yandex.practicum"})
public class WarehouseApplication {

    /**
     * Точка входа в приложение.
     */
    public static void main(String[] args) {
        SpringApplication.run(WarehouseApplication.class, args);
    }
}