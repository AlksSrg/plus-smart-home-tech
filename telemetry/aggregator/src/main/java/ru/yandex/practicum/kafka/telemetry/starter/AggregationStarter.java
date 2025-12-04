package ru.yandex.practicum.kafka.telemetry.starter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.aggregation.AggregationEventSnapshot;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.properties.KafkaProperties;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Основной компонент для запуска и управления агрегатором событий.
 * Обрабатывает события из Kafka и отправляет обновленные снапшоты.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final AggregationEventSnapshot aggregationEventSnapshot;
    private final KafkaProperties kafkaProperties;
    private final KafkaConsumer<String, SensorEventAvro> kafkaConsumer;
    private final KafkaSnapshotProducer kafkaSnapshotProducer;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ExecutorService executorService;

    /**
     * Запускает основной цикл обработки событий из Kafka.
     */
    public void start() {
        log.info("Запуск агрегатора");
        log.info("Подписка на топик: {}, отправка в: {}",
                kafkaProperties.getConsumer().getSensorsTopic(),
                kafkaProperties.getProducer().getSnapshotsTopic());

        kafkaConsumer.subscribe(Collections.singletonList(
                kafkaProperties.getConsumer().getSensorsTopic()
        ));

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::pollLoop);
    }

    /**
     * Основной цикл опроса Kafka и обработки событий.
     */
    private void pollLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, SensorEventAvro> records = kafkaConsumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    log.debug("Получено событий: {}", records.count());
                }

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    processRecord(record);
                }

                kafkaConsumer.commitSync();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Неожиданное прерывание цикла опроса", e);
            }
        } catch (Exception e) {
            log.error("Ошибка в цикле обработки событий", e);
        } finally {
            gracefulShutdown();
        }
    }

    /**
     * Обрабатывает одну запись из Kafka.
     */
    private void processRecord(ConsumerRecord<String, SensorEventAvro> record) {
        try {
            SensorEventAvro event = record.value();

            if (event == null) {
                log.warn("Получена запись с пустым значением");
                return;
            }

            Optional<SensorsSnapshotAvro> snapshot = aggregationEventSnapshot.updateState(event);

            if (snapshot.isPresent()) {
                SensorsSnapshotAvro finalSnapshot = snapshot.get();
                kafkaSnapshotProducer.sendSnapshot(finalSnapshot);
                log.info("Отправлен снапшот для хаба: {}", finalSnapshot.getHubId());
            }

        } catch (Exception e) {
            log.error("Ошибка обработки записи, топик: {}, partition: {}, offset: {}",
                    record.topic(), record.partition(), record.offset(), e);
        }
    }

    /**
     * Корректно завершает работу агрегатора.
     */
    public void shutdown() {
        log.info("Завершение работы агрегатора...");
        running.set(false);

        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }

        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * Завершение всех ресурсов.
     */
    private void gracefulShutdown() {
        try {
            if (kafkaConsumer != null) {
                kafkaConsumer.commitSync();
            }

            kafkaSnapshotProducer.close();

        } catch (Exception e) {
            log.error("Ошибка при завершении работы", e);
        } finally {
            if (kafkaConsumer != null) {
                try {
                    kafkaConsumer.close();
                    log.info("KafkaConsumer закрыт");
                } catch (Exception e) {
                    log.error("Ошибка при закрытии KafkaConsumer", e);
                }
            }

            log.info("Агрегатор завершил работу");
        }
    }
}