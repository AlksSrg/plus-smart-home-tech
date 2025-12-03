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
 * Стартер агрегатора с ручным управлением циклом опроса Kafka.
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
     * Запускает основной цикл обработки событий.
     */
    public void start() {
        log.info("Aggregator запущен");
        log.info("Подписка на: {}, отправка в: {}",
                kafkaProperties.getConsumer().getSensorsTopic(),
                kafkaProperties.getProducer().getSnapshotsTopic());

        // Подписываемся на топик
        kafkaConsumer.subscribe(Collections.singletonList(
                kafkaProperties.getConsumer().getSensorsTopic()
        ));

        // Запускаем обработку в отдельном потоке
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::pollLoop);
    }

    /**
     * Основной цикл опроса Kafka.
     */
    private void pollLoop() {
        try {
            while (running.get()) {
                // Получаем записи из Kafka
                ConsumerRecords<String, SensorEventAvro> records =
                        kafkaConsumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    log.debug("Получено {} событий для обработки", records.count());
                }

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    processRecord(record);
                }

                // Фиксируем оффсеты
                kafkaConsumer.commitSync();

            }
        } catch (WakeupException e) {
            // Игнорируем при корректном shutdown
            if (running.get()) {
                log.error("Неожиданный WakeupException", e);
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
                log.warn("Получена запись с null значением");
                return;
            }

            log.info("Получено событие - тип: {}, хаб: {}, датчик: {}, время: {}, ключ: {}, partition: {}, offset: {}",
                    event.getSchema().getName(),
                    event.getHubId(),
                    event.getId(),
                    event.getTimestamp(),
                    record.key(),
                    record.partition(),
                    record.offset());

            log.debug("Обработка события: хаб={}, датчик={}, partition={}, offset={}",
                    event.getHubId(), event.getId(),
                    record.partition(), record.offset());

            // Обновляем состояние
            Optional<SensorsSnapshotAvro> snapshot = aggregationEventSnapshot.updateState(event);

            if (snapshot.isPresent()) {
                SensorsSnapshotAvro finalSnapshot = snapshot.get();

                // Отправляем снапшот в Kafka
                kafkaSnapshotProducer.sendSnapshot(finalSnapshot);

                log.info("Обновлен снапшот для хаба {}, датчиков: {}",
                        finalSnapshot.getHubId(),
                        finalSnapshot.getSensorsState().size());
            } else {
                log.debug("Снапшот не изменился для хаба {}", event.getHubId());
            }

        } catch (Exception e) {
            log.error("Ошибка обработки записи из топика {}, partition {}, offset {}",
                    record.topic(), record.partition(), record.offset(), e);
            // Не фиксируем оффсет при ошибке для повторной обработки
        }
    }

    /**
     * Корректно завершает работу агрегатора.
     */
    public void shutdown() {
        log.info("Начинаем корректное завершение работы агрегатора...");
        running.set(false);

        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }

        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * Корректное завершение всех ресурсов.
     */
    private void gracefulShutdown() {
        try {
            // Фиксируем последние оффсеты
            if (kafkaConsumer != null) {
                kafkaConsumer.commitSync();
            }

            // Закрываем продюсер
            kafkaSnapshotProducer.close();

        } catch (Exception e) {
            log.error("Ошибка при завершении работы", e);
        } finally {
            // Закрываем консьюмер
            if (kafkaConsumer != null) {
                try {
                    kafkaConsumer.close();
                    log.info("KafkaConsumer закрыт");
                } catch (Exception e) {
                    log.error("Ошибка при закрытии KafkaConsumer", e);
                }
            }

            log.info("Aggregator корректно завершил работу");
        }
    }
}