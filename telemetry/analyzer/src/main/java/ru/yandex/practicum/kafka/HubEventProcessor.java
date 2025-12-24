package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.HubEventServiceMap;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Процессор для обработки событий от хабов из Kafka.
 * Читает сообщения из топика и делегирует обработку соответствующим сервисам.
 */
@Slf4j
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventServiceMap hubEventServiceMap;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private volatile boolean isRunning = true;
    private final String topic;

    /**
     * Создает новый процессор событий хабов.
     *
     * @param consumer консьюмер Kafka
     * @param hubEventServiceMap карта сервисов для обработки событий
     * @param topic имя топика Kafka для подписки
     */
    public HubEventProcessor(Consumer<String, HubEventAvro> consumer,
                             HubEventServiceMap hubEventServiceMap,
                             String topic) {
        this.consumer = consumer;
        this.hubEventServiceMap = hubEventServiceMap;
        this.topic = topic;
    }

    @Override
    public void run() {
        start();
    }

    /**
     * Запускает основной цикл обработки сообщений.
     */
    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик событий хабов: {}", topic);

        Map<String, HubEventService> handlerMap = hubEventServiceMap.getHubMap();

        try {
            while (isRunning) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(100));
                int count = 0;

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handleRecord(record, handlerMap);
                    manageOffsets(record, count);
                    count++;
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("WakeupException — корректное завершение HubEventProcessor");
        } catch (Exception exp) {
            log.error("Ошибка при чтении данных из топика {}", topic, exp);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception exp) {
                log.warn("Ошибка при синхронном коммите", exp);
            }
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

    private void handleRecord(ConsumerRecord<String, HubEventAvro> record,
                              Map<String, HubEventService> handlerMap) {
        HubEventAvro event = record.value();
        String payloadName = event.getPayload().getClass().getSimpleName();

        HubEventService service = handlerMap.get(payloadName);
        if (service == null) {
            throw new IllegalArgumentException("Нет обработчика для типа события: " + payloadName);
        }

        service.handle(event);
        log.debug("Обработано событие: hub={}, payload={}, offset={}",
                event.getHubId(), payloadName, record.offset());
    }

    private void manageOffsets(ConsumerRecord<String, HubEventAvro> record, int count) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        currentOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));

        if (count % 2 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, ex) -> {
                if (ex != null) {
                    log.error("Ошибка commitAsync offset'ов", ex);
                }
            });
        }
    }
}