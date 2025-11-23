package ru.yandex.practicum.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.model.hub.DeviceType;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.hub.HubEventType;

/**
 * Сервис для обработки событий добавления устройств к хабу.
 * Преобразует DeviceAddedEvent в Avro формат и отправляет в Kafka.
 */
@Service
public class DeviceAddedService extends HubEventService<DeviceAddedEventAvro> {

    /**
     * Конструктор сервиса.
     *
     * @param kafkaProducerEvent компонент для отправки событий в Kafka
     * @param topicName          название топика Kafka
     */
    public DeviceAddedService(KafkaProducerEvent kafkaProducerEvent,
                              @Value("${kafka.topics.hub-events:telemetry.hubs.v1}") String topicName) {
        super(kafkaProducerEvent, topicName);
    }

    /**
     * Возвращает тип обрабатываемого события.
     *
     * @return тип события DEVICE_ADDED
     */
    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }

    /**
     * Преобразует доменное событие в Avro payload.
     *
     * @param hubEvent доменное событие добавления устройства
     * @return Avro представление события
     */
    @Override
    public DeviceAddedEventAvro mapToAvro(HubEvent hubEvent) {
        DeviceAddedEvent deviceAddedEvent = (DeviceAddedEvent) hubEvent;

        return DeviceAddedEventAvro.newBuilder()
                .setId(deviceAddedEvent.getId())
                .setType(mapToDeviceTypeAvro(deviceAddedEvent.getDeviceType()))
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
        DeviceAddedEventAvro payload = mapToAvro(hubEvent);
        return buildHubEventAvro(hubEvent, payload);
    }

    /**
     * Преобразует доменный тип устройства в Avro тип устройства.
     *
     * @param deviceType доменный тип устройства
     * @return Avro тип устройства
     * @throws IllegalArgumentException если тип устройства не поддерживается
     */
    private DeviceTypeAvro mapToDeviceTypeAvro(DeviceType deviceType) {
        return switch (deviceType) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Неподдерживаемый тип устройства: " + deviceType);
        };
    }
}