package ru.yandex.practicum.kafka.telemetry.starter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.deserializer.SensorEventDeserializer;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.service.AggregationService;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final AggregationService aggregationService;

    @Value("${topic.telemetry-sensors}")
    private String sensorsTopic;

    @Value("${aggregator.topic.telemetry-snapshots}")
    private String snapshotsTopic;

    private volatile boolean running = true;

    public void start() {
        Map<String, Object> consumerConfigs = createConsumerConfigs();
        Map<String, Object> producerConfigs = createProducerConfigs();

        try (KafkaConsumer<String, SensorEventAvro> consumer = new KafkaConsumer<>(consumerConfigs);
             KafkaProducer<String, SensorsSnapshotAvro> producer = new KafkaProducer<>(producerConfigs)) {

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            consumer.subscribe(Collections.singletonList(sensorsTopic));
            log.info("Запущен Aggregator. Подписка на топик: {}, отправка в топик: {}", sensorsTopic, snapshotsTopic);

            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets = new HashMap<>();

            while (running) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                int count = 0;
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    handleRecord(record, producer);
                    manageOffsets(record, currentOffsets, count, consumer);
                    count++;
                }

                consumer.commitAsync();
            }

        } catch (WakeupException ignored) {
            log.info("Получен сигнал WakeupException");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        }
    }

    private void handleRecord(ConsumerRecord<String, SensorEventAvro> record, Producer<String, SensorsSnapshotAvro> producer) {
        log.debug("Обработка события: топик = {}, партиция = {}, смещение = {}",
                record.topic(), record.partition(), record.offset());

        SensorEventAvro event = record.value();
        Optional<SensorsSnapshotAvro> snapshot = aggregationService.updateState(event);

        if (snapshot.isPresent()) {
            SensorsSnapshotAvro finalSnapshot = snapshot.get();
            ProducerRecord<String, SensorsSnapshotAvro> producerRecord =
                    new ProducerRecord<>(snapshotsTopic, finalSnapshot.getHubId().toString(), finalSnapshot);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Ошибка отправки снапшота для хаба {}", finalSnapshot.getHubId(), exception);
                } else {
                    log.debug("Отправлен снапшот для хаба {} в партицию {} с offset {}",
                            finalSnapshot.getHubId(), metadata.partition(), metadata.offset());
                }
            });

            log.info("Обновлен и отправлен снапшот для хаба {}, датчиков: {}",
                    finalSnapshot.getHubId(), finalSnapshot.getSensorsState().size());
        } else {
            log.debug("Снапшот не обновлен для хаба {}", event.getHubId());
        }
    }

    private void manageOffsets(ConsumerRecord<String, SensorEventAvro> record,
                               Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets,
                               int count, Consumer<String, SensorEventAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)
        );

        if (count % 10 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.warn("Ошибка во время фиксации оффсетов", exception);
                }
            });
        }
    }

    private Map<String, Object> createConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "aggregator-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", SensorEventDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", false);
        props.put("max.poll.records", 100);
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    private Map<String, Object> createProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", GeneralAvroSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    public void stop() {
        running = false;
        log.info("Получен сигнал остановки Aggregator");
    }
}