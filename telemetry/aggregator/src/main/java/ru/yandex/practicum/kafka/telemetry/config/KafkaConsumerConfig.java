package ru.yandex.practicum.kafka.telemetry.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import ru.yandex.practicum.kafka.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.properties.KafkaProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final Environment environment;

    @Bean
    public KafkaConsumer<String, SensorEventAvro> kafkaConsumer() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers() != null ?
                        kafkaProperties.getBootstrapServers() :
                        environment.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaProperties.getConsumer().getGroupId() != null ?
                        kafkaProperties.getConsumer().getGroupId() :
                        environment.getProperty("spring.kafka.consumer.group-id", "aggregator-group"));

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                kafkaProperties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                kafkaProperties.getConsumer().getMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // Schema Registry для десериализатора
        String schemaRegistryUrl = kafkaProperties.getSchemaRegistryUrl() != null ?
                kafkaProperties.getSchemaRegistryUrl() :
                environment.getProperty("spring.kafka.properties.schema.registry.url",
                        "http://localhost:8081");
        props.put("schema.registry.url", schemaRegistryUrl);

        return new KafkaConsumer<>(props);
    }
}