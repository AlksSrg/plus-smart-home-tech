package ru.yandex.practicum.service;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/**
 * Интерфейс для обработчиков событий от хабов.
 * Каждый обработчик реализует логику для определенного типа события.
 */
public interface HubEventService {

    /**
     * Возвращает тип полезной нагрузки (payload), который обрабатывает данный сервис.
     *
     * @return простой класс имя типа события (например, "DeviceAddedEventAvro")
     */
    String getPayloadType();

    /**
     * Обрабатывает событие от хаба.
     *
     * @param hub событие от хаба для обработки
     */
    void handle(HubEventAvro hub);
}