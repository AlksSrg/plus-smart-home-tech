package ru.yandex.practicum.service.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.hub.HubEvent;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HubEventProcessingService {

    private final Map<String, HubEventService> hubEventServices;

    @Autowired
    public HubEventProcessingService(List<HubEventService> services) {
        this.hubEventServices = services.stream()
                .collect(Collectors.toMap(
                        service -> service.getType().name(),
                        Function.identity()
                ));
    }

    public void process(HubEvent event) {
        String eventType = event.getType().name();

        HubEventService service = hubEventServices.get(eventType);
        if (service != null) {
            service.handle(event);
        } else {
            log.warn("No processor found for hub event type: {}", eventType);
            throw new IllegalArgumentException("Unsupported hub event type: " + eventType);
        }
    }
}