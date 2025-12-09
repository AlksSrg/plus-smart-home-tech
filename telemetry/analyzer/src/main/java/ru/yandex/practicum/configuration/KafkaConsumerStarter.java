package ru.yandex.practicum.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.HubEventProcessor;
import ru.yandex.practicum.kafka.SnapshotProcessor;
import ru.yandex.practicum.kafka.deserializer.HubEventDeserializer;
import ru.yandex.practicum.kafka.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.HubEventServiceMap;
import ru.yandex.practicum.service.SnapshotService;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerStarter {

    private final Environment environment;
    private final ApplicationContext applicationContext;
    private final HubEventServiceMap hubEventServiceMap;
    private final SnapshotService snapshotService;

    private final AtomicInteger threadCounter = new AtomicInteger(0);

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumers() {
        try {
            log.info("Запуск Kafka консьюмеров...");

            // Запуск обработчика событий хабов
            Thread hubEventThread = new Thread(() -> {
                Consumer<String, HubEventAvro> consumer = createHubEventConsumer("hub-event-" + threadCounter.incrementAndGet());
                HubEventProcessor processor = new HubEventProcessor(
                        consumer,
                        hubEventServiceMap,
                        environment.getProperty("analyzer.topic.hub-event-topic")
                );
                processor.start();
            }, "HubEventProcessor-1");

            hubEventThread.setDaemon(true);
            hubEventThread.start();
            log.info("Поток HubEventProcessor-1 запущен");

            // Запуск обработчика снапшотов
            Thread snapshotThread = new Thread(() -> {
                Consumer<String, SensorsSnapshotAvro> consumer = createSnapshotConsumer("snapshot-" + threadCounter.incrementAndGet());
                SnapshotProcessor processor = new SnapshotProcessor(
                        consumer,
                        snapshotService,
                        environment.getProperty("analyzer.topic.snapshots-topic")
                );
                processor.start();
            }, "SnapshotProcessor-1");

            snapshotThread.setDaemon(true);
            snapshotThread.start();
            log.info("Поток SnapshotProcessor-1 запущен");

            log.info("Kafka консьюмеры успешно запущены");

        } catch (Exception e) {
            log.error("Ошибка при запуске Kafka консьюмеров", e);
        }
    }

    private Consumer<String, HubEventAvro> createHubEventConsumer(String clientIdSuffix) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("kafka.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                HubEventDeserializer.class.getName());
        config.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "analyzer-hub-consumer-" + clientIdSuffix);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("kafka.group-id.hub"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(config);
    }

    private Consumer<String, SensorsSnapshotAvro> createSnapshotConsumer(String clientIdSuffix) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("kafka.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                SensorsSnapshotDeserializer.class.getName());
        config.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "analyzer-snapshot-consumer-" + clientIdSuffix);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty("kafka.group-id.snapshot"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(config);
    }
}