package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.ProductDTO;
import ru.yandex.practicum.entity.ProductEntity;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductState;
import ru.yandex.practicum.enums.QuantityState;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.repository.ProductRepository;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    public PageProductDTO getProductsByCategory(ProductCategory category, Pageable pageable) {
        Page<ProductEntity> page = productRepository.findByProductCategory(category, pageable);
        return productMapper.toPageDTO(page);
    }

    public ProductDTO getProductById(UUID productId) {
        return productRepository.findByProductId(productId)
                .map(productMapper::toDTO)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));
    }

    @Transactional
    public ProductDTO createProduct(ProductDTO productDTO) {
        if (productRepository.existsByProductName(productDTO.getProductName())) {
            throw new RuntimeException("Product with name '" + productDTO.getProductName() + "' already exists");
        }

        if (productDTO.getPrice() == null || productDTO.getPrice().compareTo(BigDecimal.ONE) < 0) {
            throw new RuntimeException("Price must be at least 1");
        }

        ProductEntity entity = productMapper.toEntity(productDTO);

        // Установка значений по умолчанию
        if (entity.getProductState() == null) {
            entity.setProductState(ProductState.ACTIVE);
        }
        if (entity.getQuantityState() == null) {
            entity.setQuantityState(QuantityState.ENDED);
        }

        ProductEntity saved = productRepository.save(entity);
        return productMapper.toDTO(saved);
    }

    @Transactional
    public ProductDTO updateProduct(UUID productId, ProductDTO productDTO) {
        ProductEntity existing = productRepository.findByProductId(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));

        // Проверка уникальности имени, если оно изменилось
        if (productDTO.getProductName() != null &&
                !existing.getProductName().equals(productDTO.getProductName()) &&
                productRepository.existsByProductName(productDTO.getProductName())) {
            throw new RuntimeException("Product with name '" + productDTO.getProductName() + "' already exists");
        }

        // Проверка цены
        if (productDTO.getPrice() != null && productDTO.getPrice().compareTo(BigDecimal.ONE) < 0) {
            throw new RuntimeException("Price must be at least 1");
        }

        // Обновление через MapStruct
        productMapper.updateProductFromDto(existing, productDTO);

        ProductEntity updated = productRepository.save(existing);
        return productMapper.toDTO(updated);
    }

    @Transactional
    public void deactivateProduct(UUID productId) {
        ProductEntity product = productRepository.findByProductId(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));

        if (product.getProductState() == ProductState.DEACTIVATE) {
            return;
        }

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Product deactivated: {}", productId);
    }

    @Transactional
    public Boolean setQuantityState(UUID productId, QuantityState quantityState) {
        ProductEntity product = productRepository.findByProductId(productId)
                .orElseThrow(() -> new ProductNotFoundException("Product not found with id: " + productId));

        product.setQuantityState(quantityState);
        productRepository.save(product);
        log.info("Product quantity state updated: {} -> {}", productId, quantityState);
        return true;
    }
}