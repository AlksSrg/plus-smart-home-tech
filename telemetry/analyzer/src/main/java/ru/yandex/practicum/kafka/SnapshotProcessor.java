package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;

/**
 * Процессор для обработки снапшотов из Kafka.
 * Читает сообщения из топика снапшотов и делегирует обработку сервису.
 */
@Slf4j
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotService snapshotService;
    private volatile boolean isRunning = true;
    private final String topic;

    /**
     * Создает новый процессор снапшотов.
     *
     * @param consumer консьюмер Kafka
     * @param snapshotService сервис для обработки снапшотов
     * @param topic имя топика Kafka для подписки
     */
    public SnapshotProcessor(Consumer<String, SensorsSnapshotAvro> consumer,
                             SnapshotService snapshotService,
                             String topic) {
        this.consumer = consumer;
        this.snapshotService = snapshotService;
        this.topic = topic;
    }

    /**
     * Запускает основной цикл обработки сообщений.
     */
    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик снапшотов: {}", topic);

        try {
            while (isRunning) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("Wakeup — завершение SnapshotProcessor");
        } catch (Exception exp) {
            log.error("Ошибка чтения из топика {}", topic, exp);
        } finally {
            try {
                consumer.close();
            } catch (Exception exp) {
                log.warn("Ошибка при закрытии консьюмера", exp);
            }
        }
    }

    /**
     * Останавливает процессор.
     */
    @PreDestroy
    public void shutdown() {
        isRunning = false;
        consumer.wakeup();
    }

    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        snapshotService.handle(snapshot);
        log.debug("Обработан снапшот: hub={}, offset={}", snapshot.getHubId(), record.offset());
    }
}