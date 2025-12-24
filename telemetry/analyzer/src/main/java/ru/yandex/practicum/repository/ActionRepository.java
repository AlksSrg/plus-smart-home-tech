package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Action;

/**
 * Репозиторий для работы с сущностью Action.
 * Предоставляет методы для работы с действиями сценариев.
 * Наследует все стандартные методы CRUD от JpaRepository.
 */
@Repository
public interface ActionRepository extends JpaRepository<Action, Long> {
}