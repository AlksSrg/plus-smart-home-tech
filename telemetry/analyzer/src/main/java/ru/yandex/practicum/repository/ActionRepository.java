package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Action;
import ru.yandex.practicum.model.Scenario;

import java.util.List;

/**
 * Репозиторий для работы с сущностью Action.
 * Предоставляет методы для работы с действиями сценариев.
 */
public interface ActionRepository extends JpaRepository<Action, Long> {

    /**
     * Находит все действия для указанного сценария.
     *
     * @param scenario сценарий
     * @return список действий сценария
     */
    List<Action> findAllByScenario(Scenario scenario);

    /**
     * Удаляет все действия для указанного сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}