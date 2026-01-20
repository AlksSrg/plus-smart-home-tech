package ru.yandex.practicum.feign;

import feign.Response;
import feign.codec.ErrorDecoder;
import org.springframework.http.HttpStatus;
import ru.yandex.practicum.dto.ProductNotFoundResponse;

public class FeignErrorDecoder implements ErrorDecoder {

    private final ErrorDecoder defaultErrorDecoder = new Default();

    @Override
    public Exception decode(String methodKey, Response response) {
        if (response.status() == HttpStatus.NOT_FOUND.value()) {
            ProductNotFoundResponse error = new ProductNotFoundResponse();
            error.setHttpStatus(HttpStatus.NOT_FOUND);
            error.setMessage("Product not found via Feign call");
            error.setUserMessage("Товар не найден");
            return new RuntimeException(error.getMessage());
        }

        return defaultErrorDecoder.decode(methodKey, response);
    }
}