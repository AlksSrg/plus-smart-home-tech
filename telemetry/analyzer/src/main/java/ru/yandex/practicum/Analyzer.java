package ru.yandex.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.service.HubEventProcessor;
import ru.yandex.practicum.service.SnapshotProcessor;

/**
 * Основной класс приложения Analyzer.
 * Запускает Spring Boot приложение и инициализирует обработчики событий.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {

    /**
     * Точка входа в приложение.
     * Запускает Spring Boot приложение и инициализирует обработку событий.
     *
     * @param args аргументы командной строки
     */
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Analyzer.class, args);

        // Получаем бины процессоров
        final HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        final SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        log.info("Запуск сервиса Analyzer...");

        // Запускаем обработку событий хаба в отдельном потоке
        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();
        log.info("Поток обработки событий хаба запущен");

        // Запускаем обработку снапшотов в основном потоке
        log.info("Запуск обработки снапшотов...");
        snapshotProcessor.start();
    }

    /**
     * Логгер для основного класса.
     */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Analyzer.class);
}