package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.ProductDTO;
import ru.yandex.practicum.dto.SetProductQuantityStateRequest;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.service.ProductService;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    @GetMapping
    public ResponseEntity<PageProductDTO> getProductsByCategory(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") List<String> sort) {

        log.info("Method getProductsByCategory: category = {}, page = {}, size = {}, sort = {}",
                category, page, size, sort);

        Sort sorting = Sort.unsorted();
        if (sort != null && !sort.isEmpty()) {
            List<Sort.Order> orders = sort.stream()
                    .map(s -> {
                        String[] parts = s.split(",");
                        String property = parts[0];
                        Sort.Direction direction = parts.length > 1 && "desc".equalsIgnoreCase(parts[1])
                                ? Sort.Direction.DESC
                                : Sort.Direction.ASC;
                        return new Sort.Order(direction, property);
                    })
                    .collect(Collectors.toList());
            sorting = Sort.by(orders);
        }

        Pageable pageable = PageRequest.of(page, size, sorting);
        PageProductDTO result = productService.getProductsByCategory(category, pageable);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{productId}")
    public ResponseEntity<ProductDTO> getProductById(@PathVariable UUID productId) {
        log.info("Method getProductById: productId = {}", productId);
        ProductDTO product = productService.getProductById(productId);
        return ResponseEntity.ok(product);
    }

    @PutMapping
    public ResponseEntity<ProductDTO> createProduct(@Valid @RequestBody ProductDTO productDTO) {
        log.info("Method createProduct: productName = {}", productDTO.getProductName());
        ProductDTO created = productService.createProduct(productDTO);
        return ResponseEntity.ok(created);
    }

    @PostMapping
    public ResponseEntity<ProductDTO> updateProduct(@Valid @RequestBody ProductDTO productDTO) {
        log.info("Method updateProduct: productId = {}, productName = {}",
                productDTO.getProductId(), productDTO.getProductName());

        if (productDTO.getProductId() == null) {
            throw new RuntimeException("Product ID is required for update");
        }

        ProductDTO updated = productService.updateProduct(productDTO.getProductId(), productDTO);
        return ResponseEntity.ok(updated);
    }

    @PostMapping("/removeProductFromStore")
    public ResponseEntity<Boolean> removeProductFromStore(@RequestBody UUID productId) {
        log.info("Method removeProductFromStore: productId = {}", productId);
        productService.deactivateProduct(productId);
        return ResponseEntity.ok(true);
    }

    @PostMapping("/quantityState")
    public ResponseEntity<Boolean> setQuantityState(
            @Valid @RequestBody SetProductQuantityStateRequest request) {

        log.info("Method setQuantityState: productId = {}, quantityState = {}",
                request.getProductId(), request.getQuantityState());

        Boolean result = productService.setQuantityState(request.getProductId(), request.getQuantityState());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/feign/{id}")
    public ProductDTO getProductByIdFeign(@PathVariable("id") Long id) {
        log.info("Method getProductByIdFeign: id = {}", id);
        UUID uuid = convertLongToUUID(id);
        return productService.getProductById(uuid);
    }

    private UUID convertLongToUUID(Long id) {
        if (id == null) return null;
        return UUID.nameUUIDFromBytes(id.toString().getBytes());
    }
}