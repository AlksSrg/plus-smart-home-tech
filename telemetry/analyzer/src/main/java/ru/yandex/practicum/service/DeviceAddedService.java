package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.SensorRepository;

/**
 * Сервис для обработки событий добавления устройств (датчиков).
 * Сохраняет информацию о новом датчике в базу данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedService implements HubEventService {

    private final SensorRepository sensorRepository;

    @Override
    public String getPayloadType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие добавления устройства.
     *
     * @param hub событие добавления устройства
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {
        DeviceAddedEventAvro deviceAddedAvro = (DeviceAddedEventAvro) hub.getPayload();
        sensorRepository.save(
                Sensor.builder()
                        .id(deviceAddedAvro.getId())
                        .hubId(hub.getHubId())
                        .build()
        );
        log.info("Устройство с ID = {} сохранено для хаба = {}",
                deviceAddedAvro.getId(), hub.getHubId());
    }
}