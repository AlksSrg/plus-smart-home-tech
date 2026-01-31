package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * Основной класс приложения для сервиса оплаты.
 * Отвечает за обработку платежей, расчет стоимости и взаимодействие с платежными шлюзами.
 * Регистрируется в Eureka Service Discovery.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "ru.yandex.practicum.feign")
public class PaymentApplication {

    /**
     * Точка входа в приложение сервиса оплаты.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}