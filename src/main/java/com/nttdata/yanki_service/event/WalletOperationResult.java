package com.nttdata.yanki_service.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Resultado de una operación de monedero que {@code bank-account-service} publica en el topic
 * {@code wallet-operation-results}.
 * <p>
 * {@code yanki-service} lo consume para actualizar el saldo del monedero y cerrar la operación
 * pendiente, sin haber invocado ninguna API REST.
 *
 * @author NTT Data
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WalletOperationResult {

    /** Identificador de la operación de monedero a la que responde este resultado. */
    private String operationId;

    /** Estado del procesamiento: {@code COMPLETED} o {@code FAILED}. */
    private String status;

    /** Identificador del movimiento generado en la cuenta (solo si {@code COMPLETED}). */
    private String transactionId;

    /** Saldo de la cuenta después de la operación (solo si {@code COMPLETED}). */
    private Double balanceAfter;

    /** Descripción del error (solo si {@code FAILED}). */
    private String errorMessage;

    /** Fecha y hora del procesamiento en formato ISO-8601. */
    private String processedAt;
}
