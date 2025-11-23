package ru.yandex.practicum.service.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.hub.HubEvent;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Сервис для маршрутизации и обработки событий хаба.
 * Определяет соответствующий сервис для каждого типа события и делегирует обработку.
 */
@Slf4j
@Service
public class HubEventProcessingService {

    private final Map<String, HubEventService> hubEventServices;

    /**
     * Конструктор сервиса.
     *
     * @param services список всех сервисов обработки событий хаба
     */
    @Autowired
    public HubEventProcessingService(List<HubEventService> services) {
        this.hubEventServices = services.stream()
                .collect(Collectors.toMap(
                        service -> service.getType().name(),
                        Function.identity()
                ));
    }

    /**
     * Обрабатывает событие хаба, направляя его в соответствующий сервис.
     *
     * @param event событие хаба для обработки
     * @throws IllegalArgumentException если тип события не поддерживается
     * @throws RuntimeException         при ошибках обработки события
     */
    public void process(HubEvent event) {
        String eventType = event.getType().name();

        HubEventService service = hubEventServices.get(eventType);
        if (service != null) {
            service.handle(event);
        } else {
            log.warn("Не найден обработчик для типа события хаба: {}", eventType);
            throw new IllegalArgumentException("Неподдерживаемый тип события хаба: " + eventType);
        }
    }
}