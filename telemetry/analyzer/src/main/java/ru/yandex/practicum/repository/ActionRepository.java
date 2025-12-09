package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Action;

/**
 * Репозиторий для работы с сущностью Action.
 * Предоставляет методы для работы с действиями сценариев.
 */
public interface ActionRepository extends JpaRepository<Action, Long> {
}