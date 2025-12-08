package ru.yandex.practicum.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.service.HubEventProcessor;
import ru.yandex.practicum.service.SnapshotProcessor;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerStarter {

    private final HubEventProcessor hubEventProcessor;
    private final SnapshotProcessor snapshotProcessor;

    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumers() {
        try {
            log.info("Запуск Kafka consumers...");

            Thread hubEventThread = new Thread(hubEventProcessor, "HubEventProcessor");
            hubEventThread.setDaemon(true);
            hubEventThread.start();
            log.info("Поток HubEventProcessor запущен");

            Thread snapshotThread = new Thread(snapshotProcessor::start, "SnapshotProcessor");
            snapshotThread.setDaemon(true);
            snapshotThread.start();
            log.info("Поток SnapshotProcessor запущен");

            log.info("Kafka consumers успешно запущены");
        } catch (Exception e) {
            log.error("Ошибка при запуске Kafka consumers", e);
            throw new RuntimeException("Не удалось запустить Kafka consumers", e);
        }
    }
}