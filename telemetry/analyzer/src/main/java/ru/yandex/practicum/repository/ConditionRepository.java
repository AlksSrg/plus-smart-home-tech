package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Condition;

/**
 * Репозиторий для работы с сущностью Condition.
 * Предоставляет методы для работы с условиями сценариев.
 */
public interface ConditionRepository extends JpaRepository<Condition, Long> {
}