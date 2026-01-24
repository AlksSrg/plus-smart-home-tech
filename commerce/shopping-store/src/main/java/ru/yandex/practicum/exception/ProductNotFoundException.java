package ru.yandex.practicum.exception;

/**
 * Исключение, выбрасываемое при попытке доступа к товару, который не был найден в системе.
 */
public class ProductNotFoundException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением об ошибке.
     *
     * @param message детальное сообщение об ошибке, описывающее причину исключения
     */
    public ProductNotFoundException(String message) {
        super(message);
    }
}