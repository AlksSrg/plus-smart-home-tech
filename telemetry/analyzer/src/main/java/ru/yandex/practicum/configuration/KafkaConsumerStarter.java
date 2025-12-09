package ru.yandex.practicum.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.HubEventProcessor;
import ru.yandex.practicum.kafka.SnapshotProcessor;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.HubEventServiceMap;
import ru.yandex.practicum.service.SnapshotService;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerStarter {

    private final Consumer<String, HubEventAvro> hubEventConsumer;
    private final Consumer<String, SensorsSnapshotAvro> snapshotConsumer;

    private final HubEventServiceMap hubEventServiceMap;
    private final SnapshotService snapshotService;

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumers() {

        log.info("Запуск Kafka консьюмеров...");

        // --- HubEventProcessor ---
        Thread hubThread = new Thread(() -> {
            HubEventProcessor processor = new HubEventProcessor(
                    hubEventConsumer,
                    hubEventServiceMap,
                    "telemetry.hubs.v1"
            );
            processor.start();
        }, "HubEventProcessor-1");

        hubThread.setDaemon(false);
        hubThread.start();
        log.info("Поток HubEventProcessor-1 запущен");

        // --- SnapshotProcessor ---
        Thread snapshotThread = new Thread(() -> {
            SnapshotProcessor processor = new SnapshotProcessor(
                    snapshotConsumer,
                    snapshotService,
                    "telemetry.snapshots.v1"
            );
            processor.start();
        }, "SnapshotProcessor-1");

        snapshotThread.setDaemon(false);
        snapshotThread.start();
        log.info("Поток SnapshotProcessor-1 запущен");

        log.info("Kafka консьюмеры успешно запущены");
    }
}
