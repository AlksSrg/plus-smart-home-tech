package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {
    private static final Duration CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(1000);
    private final Map<String, HubEventHandler> hubHandlers;

    private final KafkaConsumer<String, HubEventAvro> consumer;

    @Value("${kafka.topics.hub}")
    private String hubTopic;

    @Autowired
    public HubEventProcessor(Set<HubEventHandler> hubHandlers,
                             KafkaConsumer<String, HubEventAvro> consumer) {
        this.hubHandlers = hubHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getEventType, Function.identity()));
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(hubTopic));
            log.info("Подписались на топик событий хаба: {}", hubTopic);

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(CONSUME_ATTEMPT_TIMEOUT);
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    String hubEventName = record.value().getPayload().getClass().getSimpleName();
                    if (hubHandlers.containsKey(hubEventName)) {
                        log.info("Обработка события хаба: {}", hubEventName);
                        hubHandlers.get(hubEventName).handleEvent(record.value());
                    } else {
                        log.warn("Неизвестное событие: {}", hubEventName);
                    }
                }
                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка при обработке событий хаба", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                log.info("Закрытие потребителя");
                consumer.close();
            }
        }
    }
}