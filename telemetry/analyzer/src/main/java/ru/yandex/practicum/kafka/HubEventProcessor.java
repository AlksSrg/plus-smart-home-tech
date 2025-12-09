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
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.HubEventServiceMap;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Процессор для обработки событий от хабов умного дома из Kafka.
 * Использует маппинг обработчиков для разных типов событий.
 */
@Slf4j
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {  // Добавить implements Runnable

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

    /**
     * Метод, вызываемый при запуске в отдельном потоке.
     */
    @Override
    public void run() {
        start();
    }

    /**
     * Запускает обработку событий от хабов.
     * Подписывается на топик и обрабатывает сообщения с использованием
     * соответствующих обработчиков для разных типов событий.
     */
    public void start() {
        consumer.subscribe(List.of(topic));
        log.info("Подписан на топик событий хабов: {}", topic);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

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
            log.info("Цикл обработки событий хабов остановлен вручную");
        } catch (WakeupException ignored) {
            log.info("Получен WakeupException для graceful shutdown");
        } catch (Exception exp) {
            log.error("Ошибка при чтении данных из топика {}", topic, exp);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception exp) {
                log.warn("Ошибка при синхронном коммите оффсетов", exp);
            } finally {
                try {
                    log.info("Закрытие консьюмера событий хабов");
                    consumer.close();
                } catch (Exception exp) {
                    log.warn("Ошибка при закрытии консьюмера", exp);
                }
            }
        }
    }

    /**
     * Останавливает обработку сообщений при уничтожении компонента.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Запущен shutdown процессора событий хабов");
        isRunning = false;
        consumer.wakeup();
    }

    /**
     * Обрабатывает одно событие от хаба.
     *
     * @param record Запись из Kafka с событием хаба
     * @param handlerMap Маппинг обработчиков по типам событий
     */
    private void handleRecord(ConsumerRecord<String, HubEventAvro> record,
                              Map<String, HubEventService> handlerMap) {
        HubEventAvro event = record.value();
        String payloadName = event.getPayload().getClass().getSimpleName();

        log.info("Получено событие от хаба: {}, тип: {}, offset: {}",
                event.getHubId(), payloadName, record.offset());

        if (handlerMap.containsKey(payloadName)) {
            handlerMap.get(payloadName).handle(event);
            log.debug("Событие успешно обработано: {}", payloadName);
        } else {
            log.error("Не найден обработчик для события типа: {}", payloadName);
            throw new IllegalArgumentException(
                    String.format("Не найден обработчик для события: %s", payloadName));
        }
    }

    /**
     * Управляет оффсетами и периодически фиксирует их.
     *
     * @param record Обработанная запись
     * @param count Счетчик обработанных записей в текущей партии
     */
    private void manageOffsets(ConsumerRecord<String, HubEventAvro> record, int count) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        currentOffsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));

        // Фиксируем оффсеты каждые 2 записи
        if (count % 2 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception == null) {
                    log.debug("Успешно зафиксированы оффсеты: {}", offsets);
                } else {
                    log.error("Ошибка при фиксации оффсетов: {}", offsets, exception);
                }
            });
        }
    }
}