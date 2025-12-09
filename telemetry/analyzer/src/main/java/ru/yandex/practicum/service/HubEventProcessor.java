package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.handler.hub.HubEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> hubConsumer;
    private final List<HubEventHandler> hubEventHandlers;

    @Value("${kafka.topics.hub}")
    private String hubTopic;

    @Override
    public void run() {
        log.info("Запуск обработчика событий от хабов");

        hubConsumer.subscribe(Collections.singletonList(hubTopic));
        log.info("Подписан на топик: {}", hubTopic);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    log.info("Получено событие от хаба: {}, тип: {}",
                            event.getHubId(), event.getPayload().getClass().getSimpleName());

                    // Находим соответствующий обработчик
                    hubEventHandlers.stream()
                            .filter(handler -> handler.getEventType().equals(event.getPayload().getClass().getSimpleName()))
                            .findFirst()
                            .ifPresentOrElse(
                                    handler -> {
                                        try {
                                            handler.handleEvent(event);
                                        } catch (Exception e) {
                                            log.error("Ошибка обработки события", e);
                                        }
                                    },
                                    () -> log.warn("Не найден обработчик для события типа: {}",
                                            event.getPayload().getClass().getSimpleName())
                            );
                }

                hubConsumer.commitSync();
            } catch (Exception e) {
                log.error("Ошибка в обработчике событий хабов", e);
            }
        }
    }
}