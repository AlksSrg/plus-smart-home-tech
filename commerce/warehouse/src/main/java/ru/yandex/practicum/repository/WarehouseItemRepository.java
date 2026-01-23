package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.WarehouseItem;

import java.util.Optional;
import java.util.UUID;

/**
 * Репозиторий для работы с товарами на складе.
 * Предоставляет методы для доступа к данным о складских позициях.
 */
@Repository
public interface WarehouseItemRepository extends JpaRepository<WarehouseItem, UUID> {

    /**
     * Находит товар на складе по ID товара.
     *
     * @param productId ID товара
     * @return Optional с найденным товаром или пустой Optional
     */
    Optional<WarehouseItem> findByProductId(UUID productId);

    /**
     * Проверяет, существует ли товар на складе по ID товара.
     *
     * @param productId ID товара
     * @return true, если товар существует на складе, иначе false
     */
    boolean existsByProductId(UUID productId);
}