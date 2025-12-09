package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Condition;

/**
 * Репозиторий для работы с сущностью Condition.
 * Предоставляет методы для работы с условиями сценариев.
 * Наследует все стандартные методы CRUD от JpaRepository.
 */
@Repository
public interface ConditionRepository extends JpaRepository<Condition, Long> {
}