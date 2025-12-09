package ru.yandex.practicum.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.HubEventProcessor;
import ru.yandex.practicum.kafka.SnapshotProcessor;

/**
 * Компонент для запуска Kafka консьюмеров после полной инициализации приложения.
 * Запускает обработчики снапшотов и событий хабов в отдельных потоках.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerStarter {

    private final HubEventProcessor hubEventProcessor;
    private final SnapshotProcessor snapshotProcessor;

    /**
     * Запускает Kafka консьюмеры после готовности приложения.
     * Создает отдельные потоки для каждого типа обработчиков.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startKafkaConsumers() {
        try {
            log.info("Запуск Kafka консьюмеров...");

            // Запуск обработчика событий хабов
            Thread hubEventThread = new Thread(() -> {
                try {
                    hubEventProcessor.start();
                } catch (Exception e) {
                    log.error("Критическая ошибка в обработчике событий хабов", e);
                }
            }, "HubEventProcessor");

            hubEventThread.setDaemon(true);
            hubEventThread.start();
            log.info("Поток HubEventProcessor запущен");

            // Запуск обработчика снапшотов
            Thread snapshotThread = new Thread(() -> {
                try {
                    snapshotProcessor.start();
                } catch (Exception e) {
                    log.error("Критическая ошибка в обработчике снапшотов", e);
                }
            }, "SnapshotProcessor");

            snapshotThread.setDaemon(true);
            snapshotThread.start();
            log.info("Поток SnapshotProcessor запущен");

            log.info("Kafka консьюмеры успешно запущены");

        } catch (Exception e) {
            log.error("Ошибка при запуске Kafka консьюмеров", e);
            throw new RuntimeException("Не удалось запустить Kafka консьюмеры", e);
        }
    }
}