package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimensionDto {
    @NotNull
    @Min(value = 1, message = "Ширина должна быть не менее 1")
    private Double width;

    @NotNull
    @Min(value = 1, message = "Высота должна быть не менее 1")
    private Double height;

    @NotNull
    @Min(value = 1, message = "Глубина должна быть не менее 1")
    private Double depth;
}