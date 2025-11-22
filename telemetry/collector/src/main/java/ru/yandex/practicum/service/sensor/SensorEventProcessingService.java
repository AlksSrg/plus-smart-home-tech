package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensor.SensorEvent;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventProcessingService {

    private final List<SensorEventService> sensorEventServices;

    public void process(SensorEvent event) {
        String eventType = event.getType().name();

        sensorEventServices.stream()
                .filter(service -> service.supports(eventType))
                .findFirst()
                .ifPresentOrElse(
                        service -> service.process(event),
                        () -> {
                            log.warn("No processor found for sensor event type: {}", eventType);
                            throw new IllegalArgumentException("Unsupported sensor event type: " + eventType);
                        }
                );
    }
}