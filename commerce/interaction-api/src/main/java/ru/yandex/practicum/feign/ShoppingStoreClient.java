package ru.yandex.practicum.feign;

import feign.FeignException;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.dto.ProductDTO;

import jakarta.validation.Valid;

import java.util.UUID;

@FeignClient(
        name = "shopping-store",
        path = "/api/v1/shopping-store",
        configuration = FeignClientConfig.class
)
public interface ShoppingStoreClient {

    @GetMapping("/{productId}")
    ProductDTO getProductById(@PathVariable("productId") UUID productId) throws FeignException;

    @PutMapping
    ProductDTO createProduct(@Valid @RequestBody ProductDTO productDTO) throws FeignException;

    @GetMapping("/feign/{id}")
    ProductDTO getProductByIdFeign(@PathVariable("id") Long id) throws FeignException;

    @GetMapping
    ProductDTO getAllProducts(
            @RequestParam ProductCategory category,
            Pageable pageable
    ) throws FeignException;
}