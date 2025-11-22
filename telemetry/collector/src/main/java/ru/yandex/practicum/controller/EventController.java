package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.service.hub.HubEventProcessingService;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.service.sensor.SensorEventProcessingService;

/**
 * REST контроллер для приема событий от датчиков и хабов.
 * Обеспечивает валидацию и маршрутизацию событий к соответствующим сервисам.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class EventController {

    private final SensorEventProcessingService sensorEventProcessingService;
    private final HubEventProcessingService hubEventProcessingService;

    /**
     * Принимает события от датчиков.
     *
     * @param event событие датчика
     * @return ResponseEntity со статусом обработки
     */
    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Получено событие датчика: {}", event);
        try {
            sensorEventProcessingService.process(event);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Ошибка обработки события датчика: {}", event, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Принимает события от хабов.
     *
     * @param event событие хаба
     * @return ResponseEntity со статусом обработки
     */
    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Получено событие хаба: {}", event);
        try {
            hubEventProcessingService.process(event);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Ошибка обработки события хаба: {}", event, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}