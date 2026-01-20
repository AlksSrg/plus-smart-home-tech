package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.PageProductDTO;
import ru.yandex.practicum.dto.SetProductQuantityStateRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@FeignClient(
        name = "shopping-cart",
        path = "/api/v1/shopping-cart",
        configuration = FeignClientConfig.class
)
public interface CartFeignClient {

    @GetMapping
    PageProductDTO getShoppingCart(@RequestParam String userName) throws FeignException;

    @PutMapping
    PageProductDTO addProductInCart(
            @RequestParam String userName,
            @RequestBody @NotEmpty Map<UUID, @NotNull @Positive Integer> products
    ) throws FeignException;

    @DeleteMapping
    void deactivationCart(@RequestParam String userName) throws FeignException;

    @PostMapping("/remove")
    PageProductDTO removeProductFromCart(
            @RequestParam String userName,
            @RequestBody @NotEmpty List<UUID> productsIds
    ) throws FeignException;

    @PostMapping("/change-quantity")
    PageProductDTO changeQuantityInCart(
            @RequestParam String userName,
            @Valid @RequestBody SetProductQuantityStateRequest quantityRequest
    ) throws FeignException;
}