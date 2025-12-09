package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;

    @Override
    public void handleEvent(HubEventAvro event) {
        DeviceAddedEventAvro deviceAddedEventAvro = (DeviceAddedEventAvro) event.getPayload();
        log.info("Получено событие добавления устройства: {} для хаба: {}",
                deviceAddedEventAvro.getId(), event.getHubId());
        if (!sensorRepository.existsByIdAndHubId(deviceAddedEventAvro.getId(), event.getHubId())) {
            Sensor sensor = new Sensor();
            sensor.setId(deviceAddedEventAvro.getId());
            sensor.setHubId(event.getHubId());
            sensorRepository.save(sensor);
            log.info("Устройство успешно сохранено в базу данных: {}", sensor);
        } else {
            log.info("Устройство уже существует в базе данных: {}", deviceAddedEventAvro.getId());
        }
    }

    @Override
    public String getEventType() {
        return "DeviceAddedEventAvro";
    }
}