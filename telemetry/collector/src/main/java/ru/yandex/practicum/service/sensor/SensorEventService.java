package ru.yandex.practicum.service.sensor;

import ru.yandex.practicum.model.sensor.SensorEvent;

/**
 * Интерфейс для сервисов обработки событий датчиков.
 * Каждая реализация обрабатывает определенный тип событий датчиков.
 */
public interface SensorEventService {

    /**
     * Обрабатывает событие датчика.
     * Преобразует доменное событие в Avro формат и отправляет в Kafka.
     *
     * @param event событие датчика для обработки
     * @throws RuntimeException если произошла ошибка при обработке события
     */
    void process(SensorEvent event);

    /**
     * Проверяет, поддерживает ли сервис указанный тип события.
     *
     * @param eventType тип события для проверки
     * @return true если сервис поддерживает тип события, иначе false
     */
    boolean supports(String eventType);
}