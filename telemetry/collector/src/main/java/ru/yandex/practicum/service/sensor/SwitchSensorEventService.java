package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;
import ru.yandex.practicum.model.sensor.SwitchSensorEvent;

/**
 * Сервис для обработки событий датчиков-переключателей.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SwitchSensorEventService implements SensorEventService {

    private final KafkaProducerEvent kafkaProducer;

    @Override
    public void process(SensorEvent event) {
        SwitchSensorEvent switchEvent = (SwitchSensorEvent) event;
        log.debug("Обработка события датчика-переключателя: {}", switchEvent);

        SwitchSensorAvro switchAvro = SwitchSensorAvro.newBuilder()
                .setState(switchEvent.getState())
                .build();

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setId(switchEvent.getId())
                .setHubId(switchEvent.getHubId())
                .setTimestamp(switchEvent.getTimestamp().toEpochMilli())
                .setPayload(switchAvro)
                .build();

        kafkaProducer.send("telemetry.sensors.v1", switchEvent.getId(), sensorEventAvro);
    }

    @Override
    public boolean supports(String eventType) {
        return SensorEventType.SWITCH_SENSOR_EVENT.name().equals(eventType);
    }
}