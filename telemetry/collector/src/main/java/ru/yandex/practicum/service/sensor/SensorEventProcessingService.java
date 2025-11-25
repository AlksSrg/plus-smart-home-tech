package ru.yandex.practicum.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensor.SensorEvent;

import java.util.List;

/**
 * Сервис для маршрутизации и обработки событий датчиков.
 * Определяет соответствующий сервис для каждого типа события и делегирует обработку.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventProcessingService {

    /**
     * Список всех сервисов обработки событий датчиков.
     */
    private final List<SensorEventService> sensorEventServices;

    /**
     * Обрабатывает событие датчика, направляя его в соответствующий сервис.
     *
     * @param event событие датчика для обработки
     * @throws IllegalArgumentException если тип события не поддерживается
     * @throws RuntimeException         при ошибках обработки события
     */
    public void process(SensorEvent event) {
        String eventType = event.getType().name();

        sensorEventServices.stream()
                .filter(service -> service.supports(eventType))
                .findFirst()
                .ifPresentOrElse(
                        service -> service.process(event),
                        () -> {
                            log.warn("Не найден обработчик для типа события датчика: {}", eventType);
                            throw new IllegalArgumentException("Неподдерживаемый тип события датчика: " + eventType);
                        }
                );
    }
}