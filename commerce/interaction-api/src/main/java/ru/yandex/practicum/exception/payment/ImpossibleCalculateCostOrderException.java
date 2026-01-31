package ru.yandex.practicum.exception.payment;

/**
 * Исключение, выбрасываемое когда невозможно рассчитать стоимость заказа.
 * Возникает при отсутствии необходимых данных для расчета.
 */
public class ImpossibleCalculateCostOrderException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message сообщение об ошибке
     */
    public ImpossibleCalculateCostOrderException(String message) {
        super(message);
    }
}