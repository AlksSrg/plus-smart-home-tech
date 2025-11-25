package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.MotionSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

/**
 * Сервис для обработки событий датчиков движения.
 * Преобразует MotionSensorEvent в Avro формат и отправляет в Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MotionSensorEventService implements SensorEventService {

    /**
     * Компонент для отправки событий в Kafka.
     */
    private final KafkaProducerEvent kafkaProducer;

    /**
     * Обрабатывает событие датчика движения.
     *
     * @param event событие датчика для обработки
     * @throws RuntimeException если произошла ошибка при обработке события
     */
    @Override
    public void process(SensorEvent event) {
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
        log.debug("Событие датчика движения отправлено в Kafka. ID: {}", motionEvent.getId());
    }

    /**
     * Проверяет, поддерживает ли сервис указанный тип события.
     *
     * @param eventType тип события для проверки
     * @return true если сервис поддерживает тип события, иначе false
     */
    @Override
    public boolean supports(String eventType) {
        return SensorEventType.MOTION_SENSOR_EVENT.name().equals(eventType);
    }
}