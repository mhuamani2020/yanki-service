package com.nttdata.yanki_service.enums;

/**
 * Tipo de operación de monedero soportada por el canal Kafka entre Yanki y
 * {@code bank-account-service}.
 *
 * @author NTT Data
 */
public enum WalletOperationType {

    /** Cargar saldo al monedero debitando la cuenta de la tarjeta de débito asociada. */
    TOP_UP,

    /** Acreditar (retirar) saldo del monedero abonando la cuenta de la tarjeta asociada. */
    WITHDRAW
}
