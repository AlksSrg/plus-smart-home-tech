package ru.yandex.practicum.kafka.telemetry.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Реализация агрегации событий в снапшоты.
 */
@Slf4j
@Component
public class AggregationEventSnapshotImpl implements AggregationEventSnapshot {

    /**
     * Хранилище снапшотов по ID хаба.
     */
    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        if (event == null || event.getHubId() == null || event.getId() == null) {
            log.warn("Получено некорректное событие: {}", event);
            return Optional.empty();
        }

        String hubId = event.getHubId();
        String sensorId = event.getId();

        // Получаем или создаем новый снапшот
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, id ->
                createNewSnapshot(event)
        );

        // Проверяем нужно ли обновлять состояние
        SensorStateAvro existingState = snapshot.getSensorsState().get(sensorId);
        if (existingState != null) {
            // Проверяем timestamp и данные
            if (existingState.getTimestamp() >= event.getTimestamp() ||
                    dataEquals(existingState.getData(), event.getPayload())) {
                log.debug("Событие игнорировано для хаба {}, датчик {}", hubId, sensorId);
                return Optional.empty();
            }
        }

        // Обновляем состояние
        SensorsSnapshotAvro updatedSnapshot = updateSnapshot(snapshot, event);
        snapshots.put(hubId, updatedSnapshot);

        log.debug("Обновлен снапшот для хаба {}, датчик {}", hubId, sensorId);
        return Optional.of(updatedSnapshot);
    }

    /**
     * Создает новый снапшот для хаба.
     */
    private SensorsSnapshotAvro createNewSnapshot(SensorEventAvro event) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(event.getId(), createSensorState(event));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorStates)
                .build();
    }

    /**
     * Обновляет существующий снапшот.
     */
    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot, SensorEventAvro event) {
        Map<String, SensorStateAvro> updatedStates = new HashMap<>(snapshot.getSensorsState());
        updatedStates.put(event.getId(), createSensorState(event));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(snapshot.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(updatedStates)
                .build();
    }

    /**
     * Создает состояние датчика.
     */
    private SensorStateAvro createSensorState(SensorEventAvro event) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();
    }

    /**
     * Сравнивает данные с учетом null.
     */
    private boolean dataEquals(Object data1, Object data2) {
        if (data1 == null && data2 == null) return true;
        if (data1 == null || data2 == null) return false;
        return data1.equals(data2);
    }
}