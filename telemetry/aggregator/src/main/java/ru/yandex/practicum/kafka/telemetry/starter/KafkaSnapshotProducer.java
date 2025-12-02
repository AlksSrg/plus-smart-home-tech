package ru.yandex.practicum.kafka.telemetry.starter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.properties.KafkaProperties;

import java.util.concurrent.ExecutionException;

/**
 * Компонент для отправки снапшотов в Kafka.
 * Инкапсулирует логику отправки и обработки ошибок.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaSnapshotProducer {

    private final KafkaTemplate<String, SensorsSnapshotAvro> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    /**
     * Отправляет снапшот в Kafka.
     */
    public void sendSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            ProducerRecord<String, SensorsSnapshotAvro> record = new ProducerRecord<>(
                    kafkaProperties.getProducer().getSnapshotsTopic(),
                    snapshot.getHubId(),
                    snapshot
            );

            RecordMetadata metadata = kafkaTemplate.send(record).get().getRecordMetadata();

            log.debug("Снапшот отправлен: хаб={}, partition={}, offset={}, timestamp={}",
                    snapshot.getHubId(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Отправка снапшота прервана для хаба {}", snapshot.getHubId(), e);
        } catch (ExecutionException e) {
            log.error("Ошибка отправки снапшота для хаба {}", snapshot.getHubId(), e.getCause());
            throw new RuntimeException("Не удалось отправить снапшот в Kafka", e.getCause());
        }
    }

    /**
     * Закрывает продюсер и сбрасывает буферы.
     */
    public void close() {
        try {
            // Сбрасываем все сообщения из буфера
            kafkaTemplate.flush();
            log.info("Буферы KafkaProducer сброшены");
        } catch (Exception e) {
            log.error("Ошибка при сбросе буферов KafkaProducer", e);
        }
    }
}