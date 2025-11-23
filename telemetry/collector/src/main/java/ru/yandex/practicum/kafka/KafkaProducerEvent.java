package ru.yandex.practicum.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.exception.KafkaSendException;

import java.util.concurrent.CompletableFuture;

/**
 * Компонент для отправки событий в Kafka.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducerEvent {

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    /**
     * Отправляет сообщение в Kafka асинхронно.
     *
     * @param topic топик
     * @param key   ключ сообщения
     * @param value значение сообщения
     */
    public void send(String topic, String key, SpecificRecordBase value) {
        validateParameters(topic, key, value);

        try {
            CompletableFuture<SendResult<String, SpecificRecordBase>> future =
                    kafkaTemplate.send(topic, key, value);

            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.debug("Сообщение успешно отправлено в топик: '{}', ключ: '{}', partition: {}, offset: {}",
                            topic, key,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Не удалось отправить сообщение в топик: '{}', ключ: '{}'",
                            topic, key, exception);
                }
            });

        } catch (Exception e) {
            log.error("Ошибка при отправке сообщения в Kafka. Топик: '{}', ключ: '{}'",
                    topic, key, e);
            throw new KafkaSendException("Не удалось отправить сообщение в Kafka", e);
        }
    }

    /**
     * Отправляет сообщение в Kafka синхронно.
     *
     * @param topic топик
     * @param key   ключ сообщения
     * @param value значение сообщения
     */
    public void sendSync(String topic, String key, SpecificRecordBase value) {
        validateParameters(topic, key, value);

        try {
            SendResult<String, SpecificRecordBase> result = kafkaTemplate.send(topic, key, value).get();

            log.debug("Сообщение синхронно отправлено в топик: '{}', ключ: '{}', partition: {}, offset: {}",
                    topic, key,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Поток был прерван при отправке сообщения в Kafka. Топик: '{}', ключ: '{}'",
                    topic, key, e);
            throw new KafkaSendException("Поток прерван при отправке сообщения в Kafka", e);
        } catch (Exception e) {
            log.error("Ошибка при синхронной отправке сообщения в Kafka. Топик: '{}', ключ: '{}'",
                    topic, key, e);
            throw new KafkaSendException("Не удалось отправить сообщение в Kafka", e);
        }
    }

    /**
     * Отправляет сообщение в Kafka с использованием параметров.
     *
     * @param param параметры отправки
     */
    public void send(KafkaProducerParam param) {
        if (!param.isValid()) {
            log.error("Попытка отправки сообщения с невалидными параметрами: {}", param);
            throw new IllegalArgumentException("Невалидные параметры отправки: " + param);
        }

        try {
            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                    param.getTopic(),
                    param.getPartition(),
                    param.getTimestamp(),
                    param.getKey(),
                    param.getValue()
            );

            CompletableFuture<SendResult<String, SpecificRecordBase>> future = kafkaTemplate.send(record);

            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.debug("Сообщение с параметрами успешно отправлено. Топик: '{}', ключ: '{}', partition: {}, offset: {}",
                            param.getTopic(), param.getKey(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Не удалось отправить сообщение с параметрами. Топик: '{}', ключ: '{}'",
                            param.getTopic(), param.getKey(), exception);
                }
            });

        } catch (Exception e) {
            log.error("Ошибка при отправке сообщения с параметрами. Параметры: {}", param, e);
            throw new KafkaSendException("Не удалось отправить сообщение с параметрами", e);
        }
    }

    /**
     * Синхронизирует буферы продюсера.
     */
    public void flush() {
        try {
            kafkaTemplate.flush();
            log.debug("Буферы Kafka продюсера успешно синхронизированы");
        } catch (Exception e) {
            log.error("Ошибка при синхронизации буферов Kafka продюсера", e);
            throw new RuntimeException("Ошибка синхронизации буферов Kafka", e);
        }
    }

    /**
     * Проверяет параметры отправки.
     *
     * @param topic топик
     * @param key   ключ
     * @param value значение
     */
    private void validateParameters(String topic, String key, SpecificRecordBase value) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Топик не может быть null или пустым");
        }
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("Ключ не может быть null или пустым");
        }
        if (value == null) {
            throw new IllegalArgumentException("Значение не может быть null");
        }

        log.trace("Параметры отправки валидны. Топик: '{}', ключ: '{}', тип значения: {}",
                topic, key, value.getClass().getSimpleName());
    }
}