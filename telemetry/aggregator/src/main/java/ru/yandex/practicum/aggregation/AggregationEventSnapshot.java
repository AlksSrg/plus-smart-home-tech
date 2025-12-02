package ru.yandex.practicum.aggregation;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Optional;

/**
 * Интерфейс для агрегации событий в снапшоты.
 */
public interface AggregationEventSnapshot {

    /**
     * Обновляет состояние на основе нового события.
     *
     * @param event событие с датчика
     * @return Optional с обновленным снапшотом
     */
    Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event);
}