package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.product.PageProductDto;
import ru.yandex.practicum.dto.product.ProductDto;
import ru.yandex.practicum.dto.product.SetProductQuantityStateRequest;
import ru.yandex.practicum.entity.ProductEntity;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductState;
import ru.yandex.practicum.enums.QuantityState;
import ru.yandex.practicum.exception.shoppingStore.ProductNotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Сервис для управления товарами.
 * Предоставляет бизнес-логику для операций с товарами.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    /**
     * Получает список товаров по категории с пагинацией.
     *
     * @param category Категория товаров
     * @param pageable Параметры пагинации и сортировки
     * @return DTO со списком товаров
     */
    public PageProductDto getProductsByCategory(ProductCategory category, Pageable pageable) {
        log.debug("Получение товаров по категории: {}", category);
        Page<ProductEntity> page = productRepository.findByProductCategory(category, pageable);

        return PageProductDto.builder()
                .content(page.getContent().stream()
                        .map(productMapper::toDTO)
                        .collect(Collectors.toList()))
                .sort(page.getSort())
                .build();
    }

    /**
     * Получает товар по идентификатору.
     *
     * @param productId UUID идентификатор товара
     * @return DTO товара
     * @throws ProductNotFoundException если товар не найден
     */
    public ProductDto getProductById(UUID productId) {
        log.debug("Получение товара по ID: {}", productId);
        return productRepository.findByProductId(productId)
                .map(productMapper::toDTO)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));
    }

    /**
     * Создает новый товар.
     *
     * @param productDto DTO с данными нового товара
     * @return Созданный товар в формате DTO
     */
    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Создание товара: {}", productDto);
        ProductEntity entity = productMapper.toEntity(productDto);

        // Установка значений по умолчанию
        if (entity.getProductState() == null) {
            entity.setProductState(ProductState.ACTIVE);
        }
        if (entity.getQuantityState() == null) {
            entity.setQuantityState(QuantityState.ENDED);
        }

        ProductEntity saved = productRepository.save(entity);
        log.info("Товар создан: ID={}, имя={}", saved.getProductId(), saved.getProductName());
        return productMapper.toDTO(saved);
    }

    /**
     * Обновляет существующий товар.
     *
     * @param productDto DTO с обновленными данными
     * @return Обновленный товар в формате DTO
     */
    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Обновление товара с ID: {}", productDto.getProductId());

        ProductEntity existing = productRepository.findByProductId(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productDto.getProductId()));

        productMapper.updateProductFromDto(existing, productDto);
        ProductEntity updated = productRepository.save(existing);

        log.info("Товар обновлен: ID={}", productDto.getProductId());
        return productMapper.toDTO(updated);
    }

    /**
     * Удаляет товар (деактивирует).
     *
     * @param productId UUID идентификатор товара
     * @return true если операция успешна
     */
    @Transactional
    public Boolean removeProductById(UUID productId) {
        log.info("Удаление товара: ID={}", productId);

        ProductEntity product = productRepository.findByProductId(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);

        log.info("Товар удален: ID={}", productId);
        return true;
    }

    /**
     * Устанавливает состояние количества товара.
     *
     * @param request Запрос с ID товара и состоянием количества
     * @return true если операция успешна
     */
    @Transactional
    public Boolean setProductQuantityState(SetProductQuantityStateRequest request) {
        log.info("Установка состояния количества: ID={}, состояние={}",
                request.getProductId(), request.getQuantityState());

        ProductEntity product = productRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + request.getProductId()));

        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);

        log.debug("Состояние количества обновлено: ID={}", request.getProductId());
        return true;
    }
}