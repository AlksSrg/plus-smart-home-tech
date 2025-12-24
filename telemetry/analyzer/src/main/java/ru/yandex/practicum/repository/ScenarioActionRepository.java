package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioAction;
import ru.yandex.practicum.model.ScenarioActionId;

import java.util.List;

/**
 * Репозиторий для работы с сущностью ScenarioAction.
 * Предоставляет методы для работы со связями сценариев и действий.
 */
@Repository
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {

    /**
     * Находит все связи действий для указанного сценария.
     *
     * @param scenario сценарий
     * @return список связей действий сценария
     */
    List<ScenarioAction> findByScenario(Scenario scenario);

    /**
     * Удаляет все связи действий для указанного сценария.
     * Используется при удалении или обновлении сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}