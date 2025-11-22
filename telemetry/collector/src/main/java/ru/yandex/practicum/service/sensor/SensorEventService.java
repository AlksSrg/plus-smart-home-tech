package ru.yandex.practicum.service.sensor;

import ru.yandex.practicum.model.sensor.SensorEvent;

/**
 * Базовый интерфейс для обработки событий датчиков.
 */
public interface SensorEventService {

    /**
     * Обрабатывает событие датчика.
     *
     * @param event событие датчика
     */
    void process(SensorEvent event);

    /**
     * Поддерживает ли сервис данный тип события.
     *
     * @param eventType тип события
     * @return true если поддерживается
     */
    boolean supports(String eventType);
}