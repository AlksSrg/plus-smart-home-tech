package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

/**
 * Сервис для обработки событий климатических датчиков.
 * Преобразует ClimateSensorEvent в Avro формат и отправляет в Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClimateSensorEventService implements SensorEventService {

    /**
     * Компонент для отправки событий в Kafka.
     */
    private final KafkaProducerEvent kafkaProducer;

    /**
     * Обрабатывает событие климатического датчика.
     *
     * @param event событие датчика для обработки
     * @throws RuntimeException если произошла ошибка при обработке события
     */
    @Override
    public void process(SensorEvent event) {
        ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
        log.debug("Обработка события климатического датчика: {}", climateEvent);

        ClimateSensorAvro climateAvro = ClimateSensorAvro.newBuilder()
                .setTemperatureC(climateEvent.getTemperatureC())
                .setHumidity(climateEvent.getHumidity())
                .setCo2Level(climateEvent.getCo2Level())
                .build();

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setId(climateEvent.getId())
                .setHubId(climateEvent.getHubId())
                .setTimestamp(climateEvent.getTimestamp().toEpochMilli())
                .setPayload(climateAvro)
                .build();

        kafkaProducer.send("telemetry.sensors.v1", climateEvent.getId(), sensorEventAvro);
        log.debug("Событие климатического датчика отправлено в Kafka. ID: {}", climateEvent.getId());
    }

    /**
     * Проверяет, поддерживает ли сервис указанный тип события.
     *
     * @param eventType тип события для проверки
     * @return true если сервис поддерживает тип события, иначе false
     */
    @Override
    public boolean supports(String eventType) {
        return SensorEventType.CLIMATE_SENSOR_EVENT.name().equals(eventType);
    }
}