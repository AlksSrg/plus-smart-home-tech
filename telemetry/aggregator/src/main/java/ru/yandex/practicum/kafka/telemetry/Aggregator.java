package ru.yandex.practicum.kafka.telemetry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.kafka.telemetry.starter.AggregationStarter;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Aggregator {

    public static void main(String[] args) {
        log.info("Запуск сервиса Aggregator...");

        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Получен сигнал завершения работы...");
            AggregationStarter aggregator = context.getBean(AggregationStarter.class);
            aggregator.stop();
            context.close();
        }));

        AggregationStarter aggregator = context.getBean(AggregationStarter.class);
        aggregator.start();
    }
}