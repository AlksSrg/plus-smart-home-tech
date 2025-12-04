package ru.yandex.practicum.handler.hub;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

/**
 * Интерфейс для обработчиков событий от хаба.
 * Каждый обработчик отвечает за определенный тип события.
 */
public interface HubEventHandler {

    /**
     * Обрабатывает событие от хаба.
     *
     * @param event событие для обработки
     */
    void handleEvent(HubEventAvro event);

    /**
     * Возвращает тип события, который обрабатывает данный обработчик.
     *
     * @return тип события
     */
    String getEventType();
}