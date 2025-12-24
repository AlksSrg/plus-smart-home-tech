package ru.yandex.practicum.service;

import lombok.Getter;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Компонент для маппинга обработчиков событий хабов по типам событий.
 * Собирает все реализации HubEventService и создает карту для быстрого доступа.
 */
@Getter
@Component
public class HubEventServiceMap {

    private final Map<String, HubEventService> hubMap;

    /**
     * Создает карту обработчиков событий хабов.
     *
     * @param hubSet множество всех реализаций HubEventService, найденных Spring контекстом
     */
    public HubEventServiceMap(Set<HubEventService> hubSet) {
        this.hubMap = hubSet.stream()
                .collect(Collectors.toMap(
                        HubEventService::getPayloadType,
                        Function.identity()
                ));
    }
}