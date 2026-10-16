package com.nttdata.yanki_service.enums;

/**
 * Estado de una operación de monedero gestionada de forma asíncrona por Kafka.
 *
 * @author NTT Data
 */
public enum WalletOperationStatus {

    /** Comando enviado a bank-account-service; pendiente de resultado. */
    PENDING,

    /** bank-account-service aplicó la operación sobre la cuenta de la tarjeta. */
    COMPLETED,

    /** bank-account-service no pudo aplicar la operación (por ejemplo, saldo insuficiente). */
    FAILED
}
