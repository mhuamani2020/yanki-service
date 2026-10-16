package com.nttdata.yanki_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Entidad que representa una operación de monedero gestionada de forma asíncrona por Kafka
 * (cargar o acreditar saldo contra la tarjeta de débito asociada).
 * <p>
 * Se persiste en la colección {@code wallet_operations}. Yanki crea la operación en estado
 * {@code PENDING} al publicar el comando en {@code wallet-operations} y la cierra al recibir
 * el resultado por {@code wallet-operation-results}, aplicando entonces el movimiento de saldo
 * correspondiente sobre el {@link Wallet}.
 *
 * @author NTT Data
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "wallet_operations")
public class WalletOperation {

    /** Identificador de la operación (lo genera Yanki y lo envía como clave en Kafka). */
    @Id
    private String id;

    /** Monedero afectado por la operación. */
    private String walletId;

    /** Tipo de operación: {@code TOP_UP} o {@code WITHDRAW}. */
    private String operationType;

    /** Importe de la operación. */
    private Double amount;

    /** Estado: {@code PENDING}, {@code COMPLETED} o {@code FAILED}. */
    private String status;

    /** Tarjeta de débito asociada usada para la operación. */
    private String debitCardId;

    /** Movimiento generado en la cuenta del banco (solo si {@code COMPLETED}). */
    private String transactionId;

    /** Descripción del error (solo si {@code FAILED}). */
    private String errorMessage;

    /** Fecha y hora de creación de la operación. */
    private LocalDateTime createdAt;
}
