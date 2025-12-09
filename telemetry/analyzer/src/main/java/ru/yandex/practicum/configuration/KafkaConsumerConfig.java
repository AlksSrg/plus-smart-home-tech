package ru.yandex.practicum.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

/**
 * Конфигурация Kafka Consumer для приложения анализатора.
 * Создает отдельные консьюмеры для обработки снапшотов и событий от хабов.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final Environment environment;

    /**
     * Создает консьюмер для обработки снапшотов сенсоров.
     *
     * @return Consumer для обработки SensorsSnapshotAvro
     */
    @Bean
    public Consumer<String, SensorsSnapshotAvro> getSnapshotConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                environment.getProperty("spring.kafka.consumer.snapshots.key-deserializer"));
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                environment.getProperty("spring.kafka.consumer.snapshots.value-deserializer"));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.snapshots.client-id"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.snapshots.group-id"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(config);
    }

    /**
     * Создает консьюмер для обработки событий от хабов.
     *
     * @return Consumer для обработки HubEventAvro
     */
    @Bean
    public Consumer<String, HubEventAvro> getHubEventConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                environment.getProperty("spring.kafka.consumer.hub.key-deserializer"));
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                environment.getProperty("spring.kafka.consumer.hub.value-deserializer"));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.hub.client-id"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("spring.kafka.consumer.hub.group-id"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(config);
    }
}