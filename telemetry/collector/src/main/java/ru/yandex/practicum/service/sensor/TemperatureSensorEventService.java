package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;
import ru.yandex.practicum.model.sensor.TemperatureSensorEvent;

/**
 * Сервис для обработки событий датчиков температуры.
 * Преобразует TemperatureSensorEvent в Avro формат и отправляет в Kafka.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TemperatureSensorEventService implements SensorEventService {

    /**
     * Компонент для отправки событий в Kafka.
     */
    private final KafkaProducerEvent kafkaProducer;

    /**
     * Обрабатывает событие датчика температуры.
     *
     * @param event событие датчика для обработки
     * @throws RuntimeException если произошла ошибка при обработке события
     */
    @Override
    public void process(SensorEvent event) {
        TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
        log.debug("Обработка события датчика температуры: {}", tempEvent);

        TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                .setTemperatureC(tempEvent.getTemperatureC())
                .setTemperatureF(tempEvent.getTemperatureF())
                .build();

        SensorEventAvro sensorEventAvro = SensorEventAvro.newBuilder()
                .setId(tempEvent.getId())
                .setHubId(tempEvent.getHubId())
                .setTimestamp(tempEvent.getTimestamp().toEpochMilli())
                .setPayload(tempAvro)
                .build();

        kafkaProducer.send("telemetry.sensors.v1", tempEvent.getId(), sensorEventAvro);
        log.debug("Событие датчика температуры отправлено в Kafka. ID: {}", tempEvent.getId());
    }

    /**
     * Проверяет, поддерживает ли сервис указанный тип события.
     *
     * @param eventType тип события для проверки
     * @return true если сервис поддерживает тип события, иначе false
     */
    @Override
    public boolean supports(String eventType) {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT.name().equals(eventType);
    }
}