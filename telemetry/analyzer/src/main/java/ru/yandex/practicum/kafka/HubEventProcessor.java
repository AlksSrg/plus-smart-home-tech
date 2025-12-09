package ru.yandex.practicum.kafka;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.HubEventServiceMap;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> consumer;
    private final HubEventServiceMap hubEventServiceMap;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private volatile boolean isRunning = true;

    @Value("${analyzer.topic.hub-event-topic}")
    private String topic;

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

    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик событий хабов: {}", topic);

        Map<String, HubEventService> handlerMap = hubEventServiceMap.getHubMap();

        try {
            while (isRunning) {
                ConsumerRecords<String, HubEventAvro> records =
                        consumer.poll(Duration.ofMillis(100));
                int count = 0;

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    handleRecord(record, handlerMap);
                    manageOffsets(record, count);
                    count++;
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync();
                    log.debug("Зафиксированы оффсеты для {} событий хабов", records.count());
                }
            }
            log.info("Цикл обработки событий хабов завершён");
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
                log.info("Закрытие консьюмера событий хабов");
                consumer.close();
            } catch (Exception exp) {
                log.warn("Ошибка при закрытии консьюмера", exp);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutdown HubEventProcessor...");
        isRunning = false;
        consumer.wakeup();
    }

    private void handleRecord(ConsumerRecord<String, HubEventAvro> record,
                              Map<String, HubEventService> handlerMap) {

        HubEventAvro event = record.value();
        String payloadName = event.getPayload().getClass().getSimpleName();

        log.info("Получено событие: hub={}, payload={}, offset={}",
                event.getHubId(), payloadName, record.offset());

        HubEventService service = handlerMap.get(payloadName);
        if (service == null) {
            throw new IllegalArgumentException(
                    "Нет обработчика для типа события: " + payloadName
            );
        }

        service.handle(event);
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
