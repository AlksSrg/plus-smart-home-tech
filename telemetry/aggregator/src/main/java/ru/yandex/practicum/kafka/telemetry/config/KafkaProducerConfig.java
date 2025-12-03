package ru.yandex.practicum.kafka.telemetry.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.properties.KafkaProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация Kafka Producer.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ProducerFactory<String, SensorsSnapshotAvro> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getProducer().getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getProducer().getRetries());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProperties.getProducer().getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProperties.getProducer().getLingerMs());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProperties.getProducer().getBufferMemory());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);

        props.put("schema.registry.url", kafkaProperties.getSchemaRegistryUrl());

        return new DefaultKafkaProducerFactory<>(props);
    }
}