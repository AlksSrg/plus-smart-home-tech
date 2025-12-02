package ru.yandex.practicum.kafka.telemetry.starter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregation.AggregationEventSnapshot;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Optional;

/**
 * Стартер агрегатора с использованием Spring Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter implements CommandLineRunner {

    private final AggregationEventSnapshot aggregationEventSnapshot;
    private final KafkaTemplate<String, SensorsSnapshotAvro> kafkaTemplate;

    @Value("${topic.telemetry-sensors}")
    private String sensorsTopic;

    @Value("${aggregator.topic.telemetry-snapshots}")
    private String snapshotsTopic;

    /**
     * {@inheritDoc}
     */
    @Override
    public void run(String... args) {
        log.info("Aggregator запущен");
        log.info("Подписка на: {}, отправка в: {}", sensorsTopic, snapshotsTopic);
    }

    /**
     * Обрабатывает события с датчиков.
     *
     * @param event событие датчика
     * @param ack   подтверждение обработки
     */
    @KafkaListener(
            topics = "${topic.telemetry-sensors}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleSensorEvent(SensorEventAvro event, Acknowledgment ack) {
        try {
            log.debug("Получено событие: хаб={}, датчик={}",
                    event.getHubId(), event.getId());

            Optional<SensorsSnapshotAvro> snapshot = aggregationEventSnapshot.updateState(event);

            if (snapshot.isPresent()) {
                SensorsSnapshotAvro finalSnapshot = snapshot.get();

                kafkaTemplate.send(snapshotsTopic, finalSnapshot.getHubId(), finalSnapshot)
                        .whenComplete((result, exception) -> {
                            if (exception != null) {
                                log.error("Ошибка отправки снапшота для хаба {}",
                                        finalSnapshot.getHubId(), exception);
                            } else {
                                log.debug("Снапшот отправлен: хаб={}, partition={}, offset={}",
                                        finalSnapshot.getHubId(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            }
                        });

                log.info("Обновлен снапшот для хаба {}, датчиков: {}",
                        finalSnapshot.getHubId(),
                        finalSnapshot.getSensorsState().size());
            } else {
                log.debug("Снапшот не изменился для хаба {}", event.getHubId());
            }

            // Подтверждаем обработку
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Ошибка обработки события для хаба {}", event.getHubId(), e);
            // Не подтверждаем при ошибке для повторной обработки
        }
    }
}