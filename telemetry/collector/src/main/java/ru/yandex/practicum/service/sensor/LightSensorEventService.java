package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.LightSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

/**
 * Сервис для обработки событий датчиков освещенности.
 * Преобразует LightSensorEvent в Avro формат и отправляет в Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LightSensorEventService implements SensorEventService {

    /**
     * Компонент для отправки событий в Kafka.
     */
    private final KafkaProducerEvent kafkaProducer;

    /**
     * Обрабатывает событие датчика освещенности.
     *
     * @param event событие датчика для обработки
     * @throws RuntimeException если произошла ошибка при обработке события
     */
    @Override
    public void process(SensorEvent event) {
        LightSensorEvent lightEvent = (LightSensorEvent) event;
        log.debug("Обработка события датчика освещенности: {}", lightEvent);

        LightSensorAvro lightAvro = LightSensorAvro.newBuilder()
                .setLinkQuality(lightEvent.getLinkQuality())
                .setLuminosity(lightEvent.getLuminosity())
                .build();

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setId(lightEvent.getId())
                .setHubId(lightEvent.getHubId())
                .setTimestamp(lightEvent.getTimestamp().toEpochMilli())
                .setPayload(lightAvro)
                .build();

        kafkaProducer.send("telemetry.sensors.v1", lightEvent.getId(), sensorEventAvro);
        log.debug("Событие датчика освещенности отправлено в Kafka. ID: {}", lightEvent.getId());
    }

    /**
     * Проверяет, поддерживает ли сервис указанный тип события.
     *
     * @param eventType тип события для проверки
     * @return true если сервис поддерживает тип события, иначе false
     */
    @Override
    public boolean supports(String eventType) {
        return SensorEventType.LIGHT_SENSOR_EVENT.name().equals(eventType);
    }
}