package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.handler.snapshot.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;
    private final SnapshotHandler snapshotHandler;

    public void start() {
        log.info("Запуск обработчика снапшотов");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();
                    log.debug("Получен снапшот для хаба: {}", snapshot.getHubId());

                    try {
                        snapshotHandler.handleSnapshot(snapshot);
                    } catch (Exception e) {
                        log.error("Ошибка обработки снапшота", e);
                    }
                }

                snapshotConsumer.commitSync();
            } catch (Exception e) {
                log.error("Ошибка в обработчике снапшотов", e);
            }
        }
    }
}