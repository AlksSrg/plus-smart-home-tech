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
        try {
            // 1. Извлекаем данные из события
            String hubId = event.getHubId();
            if (hubId == null || hubId.isEmpty()) {
                log.warn("Invalid hubId in event: {}", event);
                return Optional.empty();
            }

            String sensorId = event.getId();
            if (sensorId == null || sensorId.isEmpty()) {
                log.warn("Invalid sensorId in event: {}", event);
                return Optional.empty();
            }

            long eventTimestamp = event.getTimestamp();
            Object eventPayload = event.getPayload();

            // Проверяем тип payload для диагностики
            if (eventPayload != null) {
                log.debug("Processing event - hub: {}, sensor: {}, time: {}, payload class: {}",
                        hubId, sensorId, eventTimestamp, eventPayload.getClass().getName());
            } else {
                log.warn("Event payload is null for hub: {}, sensor: {}", hubId, sensorId);
            }

            // 2. Получаем текущий снапшот или создаем новый
            SensorsSnapshotAvro currentSnapshot = snapshots.get(hubId);

            if (currentSnapshot == null) {
                // Создаем первый снапшот для хаба
                log.info("Creating FIRST snapshot for hub: {}", hubId);
                SensorsSnapshotAvro newSnapshot = createNewSnapshot(hubId, sensorId, eventTimestamp, eventPayload);
                snapshots.put(hubId, newSnapshot);
                log.info("First snapshot created for hub: {}, sensors: 1, time: {}",
                        hubId, eventTimestamp);
                return Optional.of(newSnapshot);
            }

            // 3. Проверяем нужно ли обновлять существующий снапшот
            Map<String, SensorStateAvro> currentStates = currentSnapshot.getSensorsState();
            SensorStateAvro existingState = currentStates.get(sensorId);
            boolean shouldUpdate = false;

            if (existingState == null) {
                // Новый датчик в существующем снапшоте - всегда обновляем
                log.debug("New sensor detected in existing snapshot: {}", sensorId);
                shouldUpdate = true;
            } else {
                long existingTimestamp = existingState.getTimestamp();
                Object existingData = existingState.getData();

                // Сравниваем timestamp (принимаем только более новые события)
                if (eventTimestamp > existingTimestamp) {
                    log.debug("Newer timestamp: {} > {}", eventTimestamp, existingTimestamp);
                    shouldUpdate = true;
                }
                // Если время одинаковое, сравниваем данные
                else if (eventTimestamp == existingTimestamp) {
                    if (!isDataEqual(existingData, eventPayload)) {
                        log.debug("Same timestamp but different data");
                        shouldUpdate = true;
                    } else {
                        log.debug("Same timestamp and data - no update needed");
                    }
                } else {
                    log.debug("Older timestamp: {} < {} - skipping", eventTimestamp, existingTimestamp);
                }
            }

            // 4. Обновляем если нужно
            if (shouldUpdate) {
                SensorsSnapshotAvro updatedSnapshot = updateSnapshot(
                        currentSnapshot, sensorId, eventTimestamp, eventPayload
                );
                snapshots.put(hubId, updatedSnapshot);
                log.info("Snapshot UPDATED - hub: {}, sensors: {}, time: {}",
                        hubId, updatedSnapshot.getSensorsState().size(), eventTimestamp);
                return Optional.of(updatedSnapshot);
            }

            log.debug("No update needed for hub: {}, sensor: {}", hubId, sensorId);
            return Optional.empty();

        } catch (Exception e) {
            log.error("Error processing event: {}", event, e);
            return Optional.empty();
        }
    }

    /**
     * Безопасное сравнение данных для Avro union
     */
    private boolean isDataEqual(Object data1, Object data2) {
        if (data1 == null && data2 == null) return true;
        if (data1 == null || data2 == null) return false;

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
     * Создает новый снапшот (для первого события хаба)
     */
    private SensorsSnapshotAvro createNewSnapshot(String hubId, String sensorId,
                                                  long timestamp, Object payload) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(sensorId, createSensorState(timestamp, payload));

        return SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp) // Время первого события
                .setSensorsState(sensorStates)
                .build();
    }

    /**
     * Обновляет существующий снапшот
     */
    private SensorsSnapshotAvro updateSnapshot(SensorsSnapshotAvro snapshot, String sensorId,
                                               long timestamp, Object payload) {
        // Создаем копию map для обновления
        Map<String, SensorStateAvro> updatedStates = new HashMap<>(snapshot.getSensorsState());
        updatedStates.put(sensorId, createSensorState(timestamp, payload));

        // ВАЖНО: Для обновленного снапшота используем timestamp самого нового события
        long latestTimestamp = Math.max(snapshot.getTimestamp(), timestamp);
        return SensorsSnapshotAvro.newBuilder()
                .setHubId(snapshot.getHubId())
                .setTimestamp(latestTimestamp) // Время самого свежего события
                .setSensorsState(updatedStates)
                .build();
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
     * Метод для отладки
     */
    public Map<String, SensorsSnapshotAvro> getSnapshots() {
        return new HashMap<>(snapshots);
    }
}