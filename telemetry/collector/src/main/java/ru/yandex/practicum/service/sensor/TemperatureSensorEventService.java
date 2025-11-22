package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.KafkaProducerEvent;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.model.sensor.SensorEventType;
import ru.yandex.practicum.model.sensor.TemperatureSensorEvent;

/**
 * Сервис для обработки событий датчиков температуры.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TemperatureSensorEventService implements SensorEventService {

    private final KafkaProducerEvent kafkaProducer;

    @Override
    public void process(ru.yandex.practicum.model.sensor.SensorEvent event) {
        TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
        log.debug("Обработка события датчика температуры: {}", tempEvent);

        // Для TemperatureSensorAvro создаем напрямую, так как он уже содержит все поля
        TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                .setId(tempEvent.getId())
                .setHubId(tempEvent.getHubId())
                .setTimestamp(tempEvent.getTimestamp().toEpochMilli())
                .setTemperatureC(tempEvent.getTemperatureC())
                .setTemperatureF(tempEvent.getTemperatureF())
                .build();

        // TemperatureSensorAvro отправляется напрямую, а не через SensorEventAvro
        kafkaProducer.send("telemetry.sensors.v1", tempEvent.getId(), tempAvro);
    }

    @Override
    public boolean supports(String eventType) {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT.name().equals(eventType);
    }
}