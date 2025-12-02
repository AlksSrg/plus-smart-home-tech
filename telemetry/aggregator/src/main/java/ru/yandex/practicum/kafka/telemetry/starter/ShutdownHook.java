package ru.yandex.practicum.kafka.telemetry.starter;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Компонент для обработки корректного завершения работы приложения.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShutdownHook {

    private final AggregationStarter aggregationStarter;

    /**
     * Метод вызывается при завершении работы Spring контекста.
     */
    @PreDestroy
    public void onShutdown() {
        log.info("Получен сигнал завершения работы приложения...");
        aggregationStarter.shutdown();
    }
}