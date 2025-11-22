package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.MotionSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

/**
 * Сервис для обработки событий датчиков движения.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MotionSensorEventService implements SensorEventService {

    private final KafkaProducerEvent kafkaProducer;

    @Override
    public void process(ru.yandex.practicum.model.sensor.SensorEvent event) {
        MotionSensorEvent motionEvent = (MotionSensorEvent) event;
        log.debug("Обработка события датчика движения: {}", motionEvent);

        MotionSensorAvro motionAvro = MotionSensorAvro.newBuilder()
                .setLinkQuality(motionEvent.getLinkQuality())
                .setMotion(motionEvent.getMotion())
                .setVoltage(motionEvent.getVoltage())
                .build();

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setId(motionEvent.getId())
                .setHubId(motionEvent.getHubId())
                .setTimestamp(motionEvent.getTimestamp().toEpochMilli())
                .setPayload(motionAvro)
                .build();

        kafkaProducer.send("telemetry.sensors.v1", motionEvent.getId(), sensorEventAvro);
    }

    @Override
    public boolean supports(String eventType) {
        return SensorEventType.MOTION_SENSOR_EVENT.name().equals(eventType);
    }
}