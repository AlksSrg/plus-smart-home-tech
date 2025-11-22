package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.ClimateSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

/**
 * Сервис для обработки событий климатических датчиков.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClimateSensorEventService implements SensorEventService {

    private final KafkaProducerEvent kafkaProducer;

    @Override
    public void process(ru.yandex.practicum.model.sensor.SensorEvent event) {
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
    }

    @Override
    public boolean supports(String eventType) {
        return SensorEventType.CLIMATE_SENSOR_EVENT.name().equals(eventType);
    }
}