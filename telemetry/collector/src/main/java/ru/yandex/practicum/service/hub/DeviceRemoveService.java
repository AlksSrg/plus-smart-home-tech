package ru.yandex.practicum.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.HubEventType;

/**
 * Сервис для обработки событий удаления устройств из хаба.
 * Преобразует DeviceRemovedEvent в Avro формат и отправляет в Kafka.
 */
@Service
public class DeviceRemoveService extends HubEventService<DeviceRemovedEventAvro> {

    /**
     * Конструктор сервиса.
     *
     * @param kafkaProducerEvent компонент для отправки событий в Kafka
     * @param topicName          название топика Kafka
     */
    public DeviceRemoveService(KafkaProducerEvent kafkaProducerEvent,
                               @Value("${kafka.topics.hub-events:telemetry.hubs.v1}") String topicName) {
        super(kafkaProducerEvent, topicName);
    }

    /**
     * Возвращает тип обрабатываемого события.
     *
     * @return тип события DEVICE_REMOVED
     */
    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED;
    }

    /**
     * Преобразует доменное событие в Avro payload.
     *
     * @param hubEvent доменное событие удаления устройства
     * @return Avro представление события
     */
    @Override
    public DeviceRemovedEventAvro mapToAvro(HubEvent hubEvent) {
        DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) hubEvent;

        return DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemovedEvent.getId())
                .build();
    }

    /**
     * Преобразует доменное событие в полное Avro событие хаба.
     *
     * @param hubEvent доменное событие хаба
     * @return полное Avro событие хаба
     */
    @Override
    protected HubEventAvro mapToAvroHubEvent(HubEvent hubEvent) {
        DeviceRemovedEventAvro payload = mapToAvro(hubEvent);
        return buildHubEventAvro(hubEvent, payload);
    }
}