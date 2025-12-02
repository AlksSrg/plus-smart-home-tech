package ru.yandex.practicum.kafka.telemetry.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private String bootstrapServers = "localhost:9092";
    private Consumer consumer = new Consumer();
    private Producer producer = new Producer();
    private SchemaRegistry schemaRegistry = new SchemaRegistry();

    @Data
    public static class Consumer {
        private String groupId = "aggregator-group";
        private String autoOffsetReset = "earliest";
        private Integer maxPollRecords = 100;
    }

    @Data
    public static class Producer {
        private String acks = "all";
        private Integer retries = 3;
        private Integer batchSize = 16384;
        private Integer lingerMs = 1;
        private Integer bufferMemory = 33554432;
    }

    @Data
    public static class SchemaRegistry {
        private String url = "http://localhost:8081";
    }
}