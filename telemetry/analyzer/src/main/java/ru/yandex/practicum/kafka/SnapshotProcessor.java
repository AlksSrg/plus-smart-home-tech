package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotService snapshotService;

    private volatile boolean isRunning = true;

    @Value("${analyzer.topic.snapshots-topic}")
    private String topic;

    public SnapshotProcessor(Consumer<String, SensorsSnapshotAvro> consumer,
                             SnapshotService snapshotService,
                             String topic) {
        this.consumer = consumer;
        this.snapshotService = snapshotService;
        this.topic = topic;
    }

    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик снапшотов: {}", topic);

        try {
            while (isRunning) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
            log.info("Цикл SnapshotProcessor завершён");
        } catch (WakeupException ignored) {
            log.info("Wakeup — завершение SnapshotProcessor");
        } catch (Exception exp) {
            log.error("Ошибка чтения из топика {}", topic, exp);
        } finally {
            try {
                log.info("Закрытие консьюмера снапшотов");
                consumer.close();
            } catch (Exception exp) {
                log.warn("Ошибка при закрытии консьюмера", exp);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutdown SnapshotProcessor...");
        isRunning = false;
        consumer.wakeup();
    }

    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        int count = snapshot.getSensorsState() == null
                ? 0
                : snapshot.getSensorsState().size();

        log.info("Снапшот: hub={}, sensors={}, offset={}",
                snapshot.getHubId(), count, record.offset());

        snapshotService.handle(snapshot);
    }
}