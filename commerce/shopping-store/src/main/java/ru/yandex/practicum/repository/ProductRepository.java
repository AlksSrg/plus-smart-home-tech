package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.entity.ProductEntity;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProductRepository extends JpaRepository<ProductEntity, UUID> {

    Optional<ProductEntity> findByProductId(UUID productId);

    Page<ProductEntity> findByProductCategory(ProductCategory category, Pageable pageable);

    boolean existsByProductName(String productName);

    @Modifying
    @Transactional
    @Query("UPDATE ProductEntity p SET p.productState = 'DEACTIVATE' WHERE p.productId = :productId")
    int deactivateProduct(@Param("productId") UUID productId);
}