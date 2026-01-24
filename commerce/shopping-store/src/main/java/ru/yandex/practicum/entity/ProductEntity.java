package ru.yandex.practicum.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductState;
import ru.yandex.practicum.enums.QuantityState;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Сущность, представляющая товар в системе.
 * Соответствует таблице "products" в базе данных.
 * Используется для хранения информации о товарах, их состояниях и категориях.
 *
 * @author Автор системы
 * @version 1.0
 */
@Entity
@Table(name = "products")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductEntity {

    /**
     * Уникальный идентификатор товара.
     * Генерируется автоматически с использованием UUID.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "product_id")
    private UUID productId;

    /**
     * Название товара.
     * Не может быть null.
     */
    @Column(name = "product_name", nullable = false)
    private String productName;

    /**
     * Описание товара.
     * Максимальная длина - 1000 символов.
     * Не может быть null.
     */
    @Column(name = "description", nullable = false, length = 1000)
    private String description;

    /**
     * Ссылка на изображение товара.
     * Может быть null, если изображение отсутствует.
     */
    @Column(name = "image_src")
    private String imageSrc;

    /**
     * Состояние количества товара (например, в наличии, мало и т.д.).
     * Хранится в виде строкового представления перечисления.
     * Не может быть null.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "quantity_state", nullable = false)
    private QuantityState quantityState;

    /**
     * Состояние товара (например, активен, скрыт, удален).
     * Хранится в виде строкового представления перечисления.
     * Не может быть null.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "product_state", nullable = false)
    private ProductState productState;

    /**
     * Категория товара.
     * Хранится в виде строкового представления перечисления.
     * Может быть null, если категория не определена.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "product_category")
    private ProductCategory productCategory;

    /**
     * Цена товара.
     * Точность: 10 цифр, из них 2 знака после запятой.
     * Не может быть null.
     */
    @Column(name = "price", nullable = false, precision = 10, scale = 2)
    private BigDecimal price;
}