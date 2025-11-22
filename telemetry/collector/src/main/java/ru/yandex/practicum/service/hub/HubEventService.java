package ru.yandex.practicum.service.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.HubEventType;

import java.util.Objects;

/**
 * Абстрактный базовый сервис для обработки событий хаба.
 * Предоставляет общую логику преобразования и отправки событий в Kafka.
 *
 * @param <T> тип Avro payload для конкретного события
 */
@Slf4j
@RequiredArgsConstructor
public abstract class HubEventService<T extends SpecificRecordBase> {

    protected final KafkaProducerEvent kafkaProducerEvent;
    protected final String topicName;

    /**
     * Обрабатывает событие хаба: преобразует в Avro формат и отправляет в Kafka.
     *
     * @param hubEvent событие хаба для обработки
     * @throws IllegalArgumentException если hubEvent равен null
     * @throws RuntimeException         при ошибках преобразования или отправки
     */
    public void handle(HubEvent hubEvent) {
        validateHubEvent(hubEvent);

        try {
            HubEventAvro avroEvent = mapToAvroHubEvent(hubEvent);
            sendToKafka(hubEvent, avroEvent);
            log.debug("Событие хаба успешно обработано. Тип: {}, HubId: {}",
                    getType(), hubEvent.getHubId());
        } catch (Exception e) {
            log.error("Не удалось обработать событие хаба. Тип: {}, HubId: {}",
                    getType(), hubEvent.getHubId(), e);
            throw new RuntimeException("Ошибка обработки события хаба", e);
        }
    }

    /**
     * Возвращает тип события, который обрабатывает данный сервис.
     *
     * @return тип события хаба
     */
    public abstract HubEventType getType();

    /**
     * Преобразует доменное событие в Avro payload.
     *
     * @param hubEvent доменное событие хаба
     * @return Avro payload для события
     */
    public abstract T mapToAvro(HubEvent hubEvent);

    /**
     * Преобразует доменное событие в полное Avro событие хаба.
     *
     * @param hubEvent доменное событие хаба
     * @return полное Avro событие хаба
     */
    protected abstract HubEventAvro mapToAvroHubEvent(HubEvent hubEvent);

    /**
     * Создает базовую структуру HubEventAvro с общими полями.
     *
     * @param hubEvent    исходное доменное событие
     * @param payloadAvro Avro payload события
     * @return собранное Avro событие хаба
     */
    protected HubEventAvro buildHubEventAvro(HubEvent hubEvent, T payloadAvro) {
        Objects.requireNonNull(hubEvent, "HubEvent не может быть null");
        Objects.requireNonNull(payloadAvro, "Payload не может быть null");

        return HubEventAvro.newBuilder()
                .setHubId(hubEvent.getHubId())
                .setTimestamp(hubEvent.getTimestamp().toEpochMilli())
                .setPayload(payloadAvro)
                .build();
    }

    /**
     * Отправляет событие в Kafka.
     *
     * @param hubEvent  исходное доменное событие
     * @param avroEvent Avro событие для отправки
     */
    protected void sendToKafka(HubEvent hubEvent, HubEventAvro avroEvent) {
        kafkaProducerEvent.send(topicName, hubEvent.getHubId(), avroEvent);
        log.trace("Запись успешно отправлена. HubId: {}, Топик: {}",
                hubEvent.getHubId(), topicName);
    }

    /**
     * Валидирует входное событие хаба.
     *
     * @param hubEvent событие для валидации
     * @throws IllegalArgumentException если событие невалидно
     */
    protected void validateHubEvent(HubEvent hubEvent) {
        if (hubEvent == null) {
            throw new IllegalArgumentException("HubEvent не может быть null");
        }
        if (hubEvent.getHubId() == null || hubEvent.getHubId().trim().isEmpty()) {
            throw new IllegalArgumentException("HubId не может быть null или пустым");
        }
        if (hubEvent.getTimestamp() == null) {
            throw new IllegalArgumentException("Timestamp не может быть null");
        }
    }
}