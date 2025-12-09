package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.service.SnapshotService;

import java.time.Duration;
import java.util.List;

/**
 * Процессор для обработки снапшотов состояния сенсоров из Kafka.
 * Подписывается на топик со снапшотами и обрабатывает каждое сообщение.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final Consumer<String, SensorsSnapshotAvro> consumer;
    private final SnapshotService snapshotService;
    private volatile boolean isRunning = true;

    @Value("${analyzer.topic.snapshots-topic}")
    private String topic;

    /**
     * Запускает обработку сообщений из топика снапшотов.
     * Подписывается на топик и начинает опрос сообщений в цикле.
     * Обрабатывает graceful shutdown при получении сигналов остановки.
     */
    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик снапшотов: {}", topic);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            while (isRunning) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    handleRecord(record);
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                    log.debug("Зафиксированы оффсеты для {} снапшотов", records.count());
                }
            }
            log.info("Цикл обработки снапшотов остановлен вручную");
        } catch (WakeupException ignored) {
            log.info("Получен WakeupException для graceful shutdown");
        } catch (Exception exp) {
            log.error("Ошибка при чтении данных из топика {}", topic, exp);
        } finally {
            try {
                log.info("Закрытие консьюмера снапшотов");
                consumer.close();
            } catch (Exception exp) {
                log.warn("Ошибка при закрытии консьюмера", exp);
            }
        }
    }

    /**
     * Останавливает обработку сообщений при уничтожении компонента.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Запущен shutdown процессора снапшотов");
        isRunning = false;
        consumer.wakeup();
    }

    /**
     * Обрабатывает одно сообщение со снапшотом.
     *
     * @param record Запись из Kafka со снапшотом
     */
    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        int sensorCount = snapshot.getSensorsState() != null ?
                snapshot.getSensorsState().size() : 0;

        log.info("Получен снапшот для хаба: {}, сенсоров: {}, offset: {}",
                snapshot.getHubId(), sensorCount, record.offset());

        snapshotService.handle(snapshot);
        log.debug("Снапшот успешно обработан для хаба: {}", snapshot.getHubId());
    }
}