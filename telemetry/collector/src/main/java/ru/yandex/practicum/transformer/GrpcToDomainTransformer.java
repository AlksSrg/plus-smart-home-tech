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
            case TEMPERATURE_SENSOR_EVENT:
                TemperatureSensorProto tempProto = sensorEventProto.getTemperatureSensorEvent();
                TemperatureSensorEvent tempEvent = new TemperatureSensorEvent();
                tempEvent.setId(sensorId);
                tempEvent.setHubId(hubId);
                tempEvent.setTimestamp(timestamp);
                tempEvent.setTemperatureC(tempProto.getTemperatureC());
                tempEvent.setTemperatureF(tempProto.getTemperatureF());
                return tempEvent;

            case LIGHT_SENSOR_EVENT:
                LightSensorProto lightProto = sensorEventProto.getLightSensorEvent();
                LightSensorEvent lightEvent = new LightSensorEvent();
                lightEvent.setId(sensorId);
                lightEvent.setHubId(hubId);
                lightEvent.setTimestamp(timestamp);
                lightEvent.setLinkQuality(lightProto.getLinkQuality());
                lightEvent.setLuminosity(lightProto.getLuminosity());
                return lightEvent;

            case MOTION_SENSOR_EVENT:
                MotionSensorProto motionProto = sensorEventProto.getMotionSensorEvent();
                MotionSensorEvent motionEvent = new MotionSensorEvent();
                motionEvent.setId(sensorId);
                motionEvent.setHubId(hubId);
                motionEvent.setTimestamp(timestamp);
                motionEvent.setLinkQuality(motionProto.getLinkQuality());
                motionEvent.setMotion(motionProto.getMotion());
                motionEvent.setVoltage(motionProto.getVoltage());
                return motionEvent;

            case CLIMATE_SENSOR_EVENT:
                ClimateSensorProto climateProto = sensorEventProto.getClimateSensorEvent();
                ClimateSensorEvent climateEvent = new ClimateSensorEvent();
                climateEvent.setId(sensorId);
                climateEvent.setHubId(hubId);
                climateEvent.setTimestamp(timestamp);
                climateEvent.setTemperatureC(climateProto.getTemperatureC());
                climateEvent.setHumidity(climateProto.getHumidity());
                climateEvent.setCo2Level(climateProto.getCo2Level());
                return climateEvent;

            case SWITCH_SENSOR_EVENT:
                SwitchSensorProto switchProto = sensorEventProto.getSwitchSensorEvent();
                SwitchSensorEvent switchEvent = new SwitchSensorEvent();
                switchEvent.setId(sensorId);
                switchEvent.setHubId(hubId);
                switchEvent.setTimestamp(timestamp);
                switchEvent.setState(switchProto.getState());
                return switchEvent;

            case PAYLOAD_NOT_SET:
            default:
                log.error("Неизвестный тип датчика: {}", sensorEventProto.getPayloadCase());
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
                log.error("Неизвестный тип события хаба: {}", hubEventProto.getPayloadCase());
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
            default -> {
                log.error("Неизвестный тип устройства: {}", protoType);
                throw new IllegalArgumentException("Неизвестный тип устройства: " + protoType);
            }
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

                    switch (proto.getValueCase()) {
                        case BOOL_VALUE:
                            condition.setValue(proto.getBoolValue() ? 1 : 0);
                            log.debug("Преобразованное булево значение условия: {} -> {}", proto.getBoolValue(), condition.getValue());
                            break;
                        case INT_VALUE:
                            condition.setValue(proto.getIntValue());
                            log.debug("Целочисленное значение условия: {}", proto.getIntValue());
                            break;
                        case VALUE_NOT_SET:
                        default:
                            log.warn("Значение условия не установлено, устанавливаем 0 по умолчанию");
                            condition.setValue(0);
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

                    if (proto.hasValue()) {
                        action.setValue(proto.getValue());
                        log.debug("Значение действия установлено: {}", proto.getValue());
                    } else {
                        action.setValue(null);
                        log.debug("Значение действия не установлено, оставляем null");
                    }
                    return action;
                })
                .collect(Collectors.toList());
    }
}