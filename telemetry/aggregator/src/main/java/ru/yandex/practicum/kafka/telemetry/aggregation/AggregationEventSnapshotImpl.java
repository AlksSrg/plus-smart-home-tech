package ru.yandex.practicum.kafka.telemetry.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationEventSnapshotImpl implements AggregationEventSnapshot {

    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        // 1. Валидация входных данных
        if (event == null) {
            log.warn("Received null event");
            return Optional.empty();
        }

        String hubId = event.getHubId();
        if (hubId == null || hubId.isBlank()) {
            log.warn("Invalid hubId in event: {}", event);
            return Optional.empty();
        }

        String sensorId = event.getId();
        if (sensorId == null || sensorId.isBlank()) {
            log.warn("Invalid sensorId in event: {}", event);
            return Optional.empty();
        }

        long eventTimestamp = event.getTimestamp();
        Object eventPayload = event.getPayload();
        if (eventPayload == null) {
            log.warn("Event payload is null for hub: {}, sensor: {}", hubId, sensorId);
            return Optional.empty();
        }

        log.debug("Processing event - hub: {}, sensor: {}, time: {}, payload type: {}",
                hubId, sensorId, eventTimestamp, eventPayload.getClass().getSimpleName());

        // 2. Атомарно обновляем снапшот для хаба
        return Optional.ofNullable(
                snapshots.compute(hubId, (key, existingSnapshot) -> {
                    if (existingSnapshot == null) {
                        // Создаем новый снапшот для хаба
                        log.info("Creating FIRST snapshot for hub: {}", hubId);
                        return createNewSnapshot(hubId, sensorId, eventTimestamp, eventPayload);
                    } else {
                        // Обновляем существующий снапшот
                        return updateExistingSnapshot(existingSnapshot, sensorId, eventTimestamp, eventPayload);
                    }
                })
        ).filter(snapshot -> {
            // Проверяем, действительно ли снапшот изменился
            return isSnapshotChanged(snapshots.get(hubId), snapshot);
        });
    }

    /**
     * Создает новый снапшот для хаба
     */
    private SensorsSnapshotAvro createNewSnapshot(String hubId, String sensorId,
                                                  long timestamp, Object payload) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(sensorId, createSensorState(timestamp, payload));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(sensorStates)
                .build();
    }

    /**
     * Обновляет существующий снапшот
     */
    private SensorsSnapshotAvro updateExistingSnapshot(SensorsSnapshotAvro snapshot,
                                                       String sensorId,
                                                       long eventTimestamp,
                                                       Object eventPayload) {
        Map<String, SensorStateAvro> currentStates = snapshot.getSensorsState();
        SensorStateAvro existingState = currentStates.get(sensorId);

        // Проверяем, нужно ли обновлять состояние датчика
        if (shouldUpdateSensorState(existingState, eventTimestamp, eventPayload)) {
            // Создаем копию состояний для обновления
            Map<String, SensorStateAvro> updatedStates = new HashMap<>(currentStates);
            updatedStates.put(sensorId, createSensorState(eventTimestamp, eventPayload));

            // Вычисляем новую метку времени снапшота (максимальную из всех датчиков)
            long newSnapshotTimestamp = calculateNewSnapshotTimestamp(snapshot.getTimestamp(),
                    updatedStates.values());

            return SensorsSnapshotAvro.newBuilder()
                    .setHubId(snapshot.getHubId())
                    .setTimestamp(newSnapshotTimestamp)
                    .setSensorsState(updatedStates)
                    .build();
        }

        // Если состояние не изменилось, возвращаем оригинальный снапшот
        return snapshot;
    }

    /**
     * Проверяет, нужно ли обновлять состояние датчика
     */
    private boolean shouldUpdateSensorState(SensorStateAvro existingState,
                                            long eventTimestamp,
                                            Object eventPayload) {
        if (existingState == null) {
            log.debug("New sensor detected");
            return true;
        }

        long existingTimestamp = existingState.getTimestamp();
        Object existingData = existingState.getData();

        // Сравниваем timestamp (принимаем только более новые события)
        if (eventTimestamp > existingTimestamp) {
            log.debug("Newer timestamp detected: {} > {}", eventTimestamp, existingTimestamp);
            return true;
        }

        // Если время одинаковое, сравниваем данные
        if (eventTimestamp == existingTimestamp) {
            if (!isDataEqual(existingData, eventPayload)) {
                log.debug("Same timestamp but different data");
                return true;
            }
            log.debug("Same timestamp and data - no update needed");
        } else {
            log.debug("Older timestamp: {} < {} - skipping", eventTimestamp, existingTimestamp);
        }

        return false;
    }

    /**
     * Вычисляет новую метку времени снапшота (максимальную из всех датчиков)
     */
    private long calculateNewSnapshotTimestamp(long currentSnapshotTimestamp,
                                               Iterable<SensorStateAvro> sensorStates) {
        long maxTimestamp = currentSnapshotTimestamp;
        for (SensorStateAvro state : sensorStates) {
            if (state.getTimestamp() > maxTimestamp) {
                maxTimestamp = state.getTimestamp();
            }
        }
        return maxTimestamp;
    }

    /**
     * Безопасное сравнение данных для Avro union типов
     */
    private boolean isDataEqual(Object data1, Object data2) {
        if (data1 == null && data2 == null) {
            return true;
        }
        if (data1 == null || data2 == null) {
            return false;
        }

        // Прямое сравнение через equals
        if (data1.equals(data2)) {
            return true;
        }

        // Для Avro union типов сравниваем строковое представление
        String str1 = data1.toString();
        String str2 = data2.toString();

        // Убираем лишние пробелы и сравниваем
        boolean equal = str1.trim().equals(str2.trim());
        if (!equal) {
            log.debug("Data not equal: '{}' vs '{}'", str1, str2);
        }
        return equal;
    }

    /**
     * Проверяет, изменился ли снапшот
     */
    private boolean isSnapshotChanged(SensorsSnapshotAvro oldSnapshot, SensorsSnapshotAvro newSnapshot) {
        if (oldSnapshot == null || newSnapshot == null) {
            return true;
        }

        // Сравниваем timestamp
        if (oldSnapshot.getTimestamp() != newSnapshot.getTimestamp()) {
            return true;
        }

        // Сравниваем количество датчиков
        Map<String, SensorStateAvro> oldStates = oldSnapshot.getSensorsState();
        Map<String, SensorStateAvro> newStates = newSnapshot.getSensorsState();

        if (oldStates.size() != newStates.size()) {
            return true;
        }

        // Сравниваем состояния каждого датчика
        for (Map.Entry<String, SensorStateAvro> entry : newStates.entrySet()) {
            String sensorId = entry.getKey();
            SensorStateAvro newState = entry.getValue();
            SensorStateAvro oldState = oldStates.get(sensorId);

            if (oldState == null ||
                    oldState.getTimestamp() != newState.getTimestamp() ||
                    !isDataEqual(oldState.getData(), newState.getData())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Создает состояние датчика
     */
    private SensorStateAvro createSensorState(long timestamp, Object payload) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(timestamp)
                .setData(payload)
                .build();
    }

    /**
     * Метод для отладки и тестирования
     */
    public Map<String, SensorsSnapshotAvro> getSnapshots() {
        return new HashMap<>(snapshots);
    }

    /**
     * Очищает снапшоты
     */
    public void clear() {
        snapshots.clear();
    }
}