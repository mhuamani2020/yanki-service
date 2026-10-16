package com.nttdata.yanki_service.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Comando que {@code yanki-service} publica en el topic {@code wallet-operations} para
 * solicitar a {@code bank-account-service} una operación sobre la cuenta principal asociada a
 * una tarjeta de débito.
 * <p>
 * Es el mismo contrato que consume {@code bank-account-service}; viaja como JSON en texto, de
 * modo que ambos microservicios no comparten clases Java. Cumple la regla de la Parte III:
 * <i>"Los nuevos microservicios no podrán invocar las APIs de los microservicios usando
 * peticiones REST"</i>.
 *
 * @author NTT Data
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WalletOperationEvent {

    /** Identificador único de la operación de monedero (correlaciona con el resultado). */
    private String operationId;

    /** Tipo de operación solicitada: {@code TOP_UP} o {@code WITHDRAW}. */
    private String operationType;

    /** Tarjeta de débito asociada al monedero cuya cuenta principal se afectará. */
    private String debitCardId;

    /** Importe de la operación. */
    private Double amount;

    /** Fecha y hora de la solicitud en formato ISO-8601. */
    private String requestedAt;
}
