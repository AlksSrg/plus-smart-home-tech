package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.model.Condition;
import ru.yandex.practicum.model.Scenario;

import java.util.List;

/**
 * Репозиторий для работы с сущностью Condition.
 * Предоставляет методы для работы с условиями сценариев.
 */
public interface ConditionRepository extends JpaRepository<Condition, Long> {

    /**
     * Находит все условия для указанного сценария.
     *
     * @param scenario сценарий
     * @return список условий сценария
     */
    List<Condition> findAllByScenario(Scenario scenario);

    /**
     * Удаляет все условия для указанного сценария.
     *
     * @param scenario сценарий
     */
    void deleteByScenario(Scenario scenario);
}