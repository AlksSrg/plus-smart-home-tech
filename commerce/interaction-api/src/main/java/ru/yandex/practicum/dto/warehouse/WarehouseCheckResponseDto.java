package ru.yandex.practicum.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

/**
 * DTO для ответа на проверку доступности товаров.
 * Не используется в текущем контроллере - сохранён для возможного расширения функционала.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseCheckResponseDto {

    /**
     * Флаг доступности всех товаров.
     */
    private boolean available;

    /**
     * Карта недоступных товаров.
     * Ключ - ID товара, значение - недостающее количество.
     */
    private Map<UUID, Integer> unavailableProducts;

    /**
     * Сообщение о результате проверки.
     */
    private String message;
}