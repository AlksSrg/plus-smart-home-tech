package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.entity.ProductEntity;
import ru.yandex.practicum.enums.ProductCategory;

import java.util.Optional;
import java.util.UUID;

/**
 * Репозиторий для работы с сущностями товаров.
 * Предоставляет методы для доступа к данным товаров.
 */
@Repository
public interface ProductRepository extends JpaRepository<ProductEntity, UUID> {

    /**
     * Находит товар по его идентификатору.
     *
     * @param productId UUID идентификатор товара
     * @return Optional с найденным товаром или пустой Optional
     */
    Optional<ProductEntity> findByProductId(UUID productId);

    /**
     * Находит товары по категории с пагинацией.
     *
     * @param category Категория товаров
     * @param pageable Параметры пагинации и сортировки
     * @return Страница с товарами указанной категории
     */
    Page<ProductEntity> findByProductCategory(ProductCategory category, Pageable pageable);

    /**
     * Проверяет существование товара с указанным именем.
     *
     * @param productName Название товара
     * @return true если товар с таким именем существует
     */
    boolean existsByProductName(String productName);

    /**
     * Деактивирует товар путем прямого обновления в базе данных.
     * Используется для массовых операций.
     *
     * @param productId UUID идентификатор товара
     * @return Количество обновленных записей
     */
    @Modifying
    @Transactional
    @Query("UPDATE ProductEntity p SET p.productState = 'DEACTIVATE' WHERE p.productId = :productId")
    int deactivateProduct(@Param("productId") UUID productId);

    /**
     * Проверяет существование активного товара с указанным идентификатором.
     *
     * @param productId UUID идентификатор товара
     * @return true если активный товар существует
     */
    @Query("SELECT CASE WHEN COUNT(p) > 0 THEN true ELSE false END " +
            "FROM ProductEntity p WHERE p.productId = :productId AND p.productState = 'ACTIVE'")
    boolean existsActiveProductById(@Param("productId") UUID productId);
}