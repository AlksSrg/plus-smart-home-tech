package ru.yandex.practicum.transformer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.model.hub.*;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensor.*;
import ru.yandex.practicum.model.sensor.SensorEvent;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Компонент для преобразования Protobuf сообщений в доменные модели.
 */
@Slf4j
@Component
public class GrpcToDomainTransformer {

    /**
     * Преобразует Protobuf событие датчика в доменную модель.
     *
     * @param sensorEventProto Protobuf сообщение
     * @return доменная модель SensorEvent
     */
    public SensorEvent toSensorEvent(SensorEventProto sensorEventProto) {
        String hubId = sensorEventProto.getHubId();
        String sensorId = sensorEventProto.getId();
        Instant timestamp = Instant.ofEpochSecond(
                sensorEventProto.getTimestamp().getSeconds(),
                sensorEventProto.getTimestamp().getNanos()
        );

        switch (sensorEventProto.getPayloadCase()) {
            case TEMPERATURE_SENSOR:
                TemperatureSensorProto tempProto = sensorEventProto.getTemperatureSensor();
                TemperatureSensorEvent tempEvent = new TemperatureSensorEvent();
                tempEvent.setId(sensorId);
                tempEvent.setHubId(hubId);
                tempEvent.setTimestamp(timestamp);
                tempEvent.setTemperatureC(tempProto.getTemperatureC());
                tempEvent.setTemperatureF(tempProto.getTemperatureF());
                return tempEvent;

            case LIGHT_SENSOR:
                LightSensorProto lightProto = sensorEventProto.getLightSensor();
                LightSensorEvent lightEvent = new LightSensorEvent();
                lightEvent.setId(sensorId);
                lightEvent.setHubId(hubId);
                lightEvent.setTimestamp(timestamp);
                lightEvent.setLinkQuality(lightProto.getLinkQuality());
                lightEvent.setLuminosity(lightProto.getLuminosity());
                return lightEvent;

            case MOTION_SENSOR:
                MotionSensorProto motionProto = sensorEventProto.getMotionSensor();
                MotionSensorEvent motionEvent = new MotionSensorEvent();
                motionEvent.setId(sensorId);
                motionEvent.setHubId(hubId);
                motionEvent.setTimestamp(timestamp);
                motionEvent.setLinkQuality(motionProto.getLinkQuality());
                motionEvent.setMotion(motionProto.getMotion());
                motionEvent.setVoltage(motionProto.getVoltage());
                return motionEvent;

            case CLIMATE_SENSOR:
                ClimateSensorProto climateProto = sensorEventProto.getClimateSensor();
                ClimateSensorEvent climateEvent = new ClimateSensorEvent();
                climateEvent.setId(sensorId);
                climateEvent.setHubId(hubId);
                climateEvent.setTimestamp(timestamp);
                climateEvent.setTemperatureC(climateProto.getTemperatureC());
                climateEvent.setHumidity(climateProto.getHumidity());
                climateEvent.setCo2Level(climateProto.getCo2Level());
                return climateEvent;

            case SWITCH_SENSOR:
                SwitchSensorProto switchProto = sensorEventProto.getSwitchSensor();
                SwitchSensorEvent switchEvent = new SwitchSensorEvent();
                switchEvent.setId(sensorId);
                switchEvent.setHubId(hubId);
                switchEvent.setTimestamp(timestamp);
                switchEvent.setState(switchProto.getState());
                return switchEvent;

            case PAYLOAD_NOT_SET:
            default:
                throw new IllegalArgumentException("Неизвестный тип датчика: " + sensorEventProto.getPayloadCase());
        }
    }

    /**
     * Преобразует Protobuf событие хаба в доменную модель.
     *
     * @param hubEventProto Protobuf сообщение
     * @return доменная модель HubEvent
     */
    public HubEvent toHubEvent(HubEventProto hubEventProto) {
        String hubId = hubEventProto.getHubId();
        Instant timestamp = Instant.ofEpochSecond(
                hubEventProto.getTimestamp().getSeconds(),
                hubEventProto.getTimestamp().getNanos()
        );

        switch (hubEventProto.getPayloadCase()) {
            case DEVICE_ADDED:
                DeviceAddedEventProto deviceAddedProto = hubEventProto.getDeviceAdded();
                DeviceAddedEvent deviceAddedEvent = new DeviceAddedEvent();
                deviceAddedEvent.setHubId(hubId);
                deviceAddedEvent.setTimestamp(timestamp);
                deviceAddedEvent.setId(deviceAddedProto.getId());
                deviceAddedEvent.setDeviceType(mapToDeviceType(deviceAddedProto.getType()));
                return deviceAddedEvent;

            case DEVICE_REMOVED:
                DeviceRemovedEventProto deviceRemovedProto = hubEventProto.getDeviceRemoved();
                DeviceRemovedEvent deviceRemovedEvent = new DeviceRemovedEvent();
                deviceRemovedEvent.setHubId(hubId);
                deviceRemovedEvent.setTimestamp(timestamp);
                deviceRemovedEvent.setId(deviceRemovedProto.getId());
                return deviceRemovedEvent;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenarioAddedProto = hubEventProto.getScenarioAdded();
                ScenarioAddedEvent scenarioAddedEvent = new ScenarioAddedEvent();
                scenarioAddedEvent.setHubId(hubId);
                scenarioAddedEvent.setTimestamp(timestamp);
                scenarioAddedEvent.setName(scenarioAddedProto.getName());
                scenarioAddedEvent.setConditions(mapToConditions(scenarioAddedProto.getConditionsList()));
                scenarioAddedEvent.setActions(mapToActions(scenarioAddedProto.getActionsList()));
                return scenarioAddedEvent;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventProto scenarioRemovedProto = hubEventProto.getScenarioRemoved();
                ScenarioRemovedEvent scenarioRemovedEvent = new ScenarioRemovedEvent();
                scenarioRemovedEvent.setHubId(hubId);
                scenarioRemovedEvent.setTimestamp(timestamp);
                scenarioRemovedEvent.setName(scenarioRemovedProto.getName());
                return scenarioRemovedEvent;

            case PAYLOAD_NOT_SET:
            default:
                throw new IllegalArgumentException("Неизвестный тип события хаба: " + hubEventProto.getPayloadCase());
        }
    }

    /**
     * Преобразует Protobuf тип устройства в доменный тип.
     */
    private DeviceType mapToDeviceType(DeviceTypeProto protoType) {
        return switch (protoType) {
            case MOTION_SENSOR -> DeviceType.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceType.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceType.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceType.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceType.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Неизвестный тип устройства: " + protoType);
        };
    }

    /**
     * Преобразует Protobuf условия в доменные модели.
     */
    private List<ScenarioCondition> mapToConditions(List<ScenarioConditionProto> conditionProtos) {
        return conditionProtos.stream()
                .map(proto -> {
                    ScenarioCondition condition = new ScenarioCondition();
                    condition.setSensorId(proto.getSensorId());
                    condition.setType(ConditionType.valueOf(proto.getType().name()));
                    condition.setOperation(ConditionOperation.valueOf(proto.getOperation().name()));

                    // Обрабатываем значение в зависимости от типа
                    switch (proto.getValueCase()) {
                        case BOOL_VALUE:
                            // Boolean -> Integer (true = 1, false = 0)
                            condition.setValue(proto.getBoolValue() ? 1 : 0);
                            break;
                        case INT_VALUE:
                            condition.setValue(proto.getIntValue());
                            break;
                        case VALUE_NOT_SET:
                        default:
                            // Валидация требует @NotNull, но в Protobuf может быть не установлено
                            // В реальном коде нужно решить, как обрабатывать эту ситуацию
                            condition.setValue(0); // или null, но тогда будет нарушена валидация
                            break;
                    }

                    return condition;
                })
                .collect(Collectors.toList());
    }

    /**
     * Преобразует Protobuf действия в доменные модели.
     */
    private List<DeviceAction> mapToActions(List<DeviceActionProto> actionProtos) {
        return actionProtos.stream()
                .map(proto -> {
                    DeviceAction action = new DeviceAction();
                    action.setSensorId(proto.getSensorId());
                    action.setType(ActionType.valueOf(proto.getType().name()));

                    // Для optional поля проверяем hasValue()
                    if (proto.hasValue()) {
                        action.setValue(proto.getValue());
                    } else {
                        // Валидация не требует @NotNull для DeviceAction.value
                        // Можно оставить null
                        action.setValue(null);
                    }

                    return action;
                })
                .collect(Collectors.toList());
    }
}