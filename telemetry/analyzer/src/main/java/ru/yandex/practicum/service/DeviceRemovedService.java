package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.repository.SensorRepository;

/**
 * Сервис для обработки событий удаления устройств (датчиков).
 * Удаляет информацию о датчике из базы данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedService implements HubEventService {

    private final SensorRepository sensorRepository;

    @Override
    public String getPayloadType() {
        return DeviceRemovedEventAvro.class.getSimpleName();
    }

    /**
     * Обрабатывает событие удаления устройства.
     * Удаляет датчик из базы данных с проверкой принадлежности хабу.
     *
     * @param hub событие удаления устройства
     */
    @Transactional
    @Override
    public void handle(HubEventAvro hub) {
        DeviceRemovedEventAvro deviceRemovedAvro = (DeviceRemovedEventAvro) hub.getPayload();
        log.info("Удаление устройства с ID = {} для хаба = {}",
                deviceRemovedAvro.getId(), hub.getHubId());

        sensorRepository.deleteByIdAndHubId(deviceRemovedAvro.getId(), hub.getHubId());
        log.debug("Устройство с ID = {} успешно удалено", deviceRemovedAvro.getId());
    }
}