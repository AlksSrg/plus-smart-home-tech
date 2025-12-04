package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedHandler implements HubEventHandler {
    private final SensorRepository sensorRepository;

    @Override
    public void handleEvent(HubEventAvro event) {
        DeviceRemovedEventAvro deviceRemovedEventAvro = (DeviceRemovedEventAvro) event.getPayload();
        log.info("Удаление устройства для хаба: {} и устройства: {}",
                event.getHubId(), deviceRemovedEventAvro.getId());

        sensorRepository.findByIdAndHubId(deviceRemovedEventAvro.getId(), event.getHubId())
                .ifPresent(sensor -> {
                    sensorRepository.delete(sensor);
                    log.info("Устройство успешно удалено: {}", deviceRemovedEventAvro.getId());
                });
    }

    @Override
    public String getEventType() {
        return DeviceRemovedEventAvro.class.getSimpleName();
    }
}