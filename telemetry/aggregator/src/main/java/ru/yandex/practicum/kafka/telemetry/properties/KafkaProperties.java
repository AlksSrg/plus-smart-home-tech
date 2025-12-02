package ru.yandex.practicum.kafka.telemetry.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные свойства для настройки Kafka.
 * Связывает свойства из application.properties с Java объектами.
 */
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    /**
     * Адреса брокеров Kafka в формате host:port.
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * URL Schema Registry для Avro схем.
     */
    private String schemaRegistryUrl = "http://localhost:8081";

    /**
     * Настройки consumer.
     */
    private Consumer consumer = new Consumer();

    /**
     * Настройки producer.
     */
    private Producer producer = new Producer();

    /**
     * Настройки consumer Kafka.
     */
    @Data
    public static class Consumer {

        /**
         * Идентификатор группы consumer.
         */
        private String groupId = "aggregator-group";

        /**
         * Стратегия обработки offset при их отсутствии.
         */
        private String autoOffsetReset = "earliest";

        /**
         * Максимальное количество записей для одного poll.
         */
        private Integer maxPollRecords = 100;

        /**
         * Топик для чтения событий датчиков.
         */
        private String sensorsTopic = "telemetry.sensors.v1";
    }

    /**
     * Настройки producer Kafka.
     */
    @Data
    public static class Producer {

        /**
         * Уровень подтверждения записи (acks).
         */
        private String acks = "all";

        /**
         * Количество повторных попыток отправки при ошибке.
         */
        private Integer retries = 3;

        /**
         * Размер батча для отправки в байтах.
         */
        private Integer batchSize = 16384;

        /**
         * Задержка перед отправкой батча в миллисекундах.
         */
        private Integer lingerMs = 1;

        /**
         * Объем памяти для буферизации отправляемых сообщений.
         */
        private Integer bufferMemory = 33554432;

        /**
         * Топик для отправки снапшотов.
         */
        private String snapshotsTopic = "telemetry.snapshots.v1";
    }
}