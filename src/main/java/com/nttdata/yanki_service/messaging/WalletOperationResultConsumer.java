package com.nttdata.yanki_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nttdata.yanki_service.enums.WalletOperationStatus;
import com.nttdata.yanki_service.enums.WalletOperationType;
import com.nttdata.yanki_service.event.WalletOperationResult;
import com.nttdata.yanki_service.model.Wallet;
import com.nttdata.yanki_service.model.WalletOperation;
import com.nttdata.yanki_service.repository.WalletOperationRepository;
import com.nttdata.yanki_service.repository.WalletRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de los resultados de operaciones de monedero publicados por
 * {@code bank-account-service} en Kafka (Parte III).
 * <p>
 * Escucha el topic {@code wallet-operation-results}. Por cada resultado cierra la operación
 * pendiente y, si se completó, actualiza el saldo del monedero:
 * <ul>
 *     <li>{@code TOP_UP}: el banco debitó la cuenta de la tarjeta, por lo que se <b>abona</b>
 *     el importe al monedero;</li>
 *     <li>{@code WITHDRAW}: el banco abonó la cuenta de la tarjeta, por lo que se <b>debita</b>
 *     el importe del monedero.</li>
 * </ul>
 * La lógica se implementa con <b>RxJava 3</b>; los repositorios reactivos de Reactor se
 * adaptan en el borde con {@code Single.fromPublisher(...)}.
 *
 * @author NTT Data
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WalletOperationResultConsumer {

    private final WalletOperationRepository walletOperationRepository;
    private final WalletRepository walletRepository;
    private final ObjectMapper objectMapper;

    /**
     * Procesa un resultado de operación de monedero recibido desde Kafka.
     *
     * @param payload evento {@link WalletOperationResult} serializado en JSON
     */
    @KafkaListener(topics = "${app.kafka.topics.wallet-operation-results}")
    public void onWalletOperationResult(String payload) {
        log.info("Received wallet operation result: {}", payload);
        WalletOperationResult result = deserialize(payload);
        if (result.getOperationId() == null) {
            log.error("Discarding wallet operation result without operationId");
            return;
        }
        applyResult(result).subscribe(() -> { },
                error -> log.error("Error applying wallet operation result: {}", error.getMessage(), error));
    }

    /**
     * Deserializa el mensaje proveniente de Kafka.
     *
     * @param payload JSON recibido
     * @return resultado de la operación; si el JSON es inválido devuelve un resultado vacío
     */
    private WalletOperationResult deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, WalletOperationResult.class);
        } catch (Exception ex) {
            log.error("Could not deserialize wallet operation result: {}", ex.getMessage());
            return WalletOperationResult.builder().build();
        }
    }

    /**
     * Cierra la operación pendiente y, si procede, actualiza el saldo del monedero.
     *
     * @param result resultado recibido
     * @return {@code Completable} que notifica la finalización
     */
    private Completable applyResult(WalletOperationResult result) {
        return Maybe.fromPublisher(walletOperationRepository.findById(result.getOperationId()))
                .switchIfEmpty(Maybe.<WalletOperation>error(
                        new IllegalStateException("Operación de monedero no encontrada: " + result.getOperationId())))
                .flatMapCompletable(operation -> {
                    boolean completed = WalletOperationStatus.COMPLETED.name().equals(result.getStatus());
                    operation.setStatus(completed
                            ? WalletOperationStatus.COMPLETED.name()
                            : WalletOperationStatus.FAILED.name());
                    operation.setTransactionId(result.getTransactionId());
                    operation.setErrorMessage(result.getErrorMessage());

                    if (!completed) {
                        return persistOperation(operation);
                    }
                    return applyBalanceAndPersist(operation);
                });
    }

    /**
     * Actualiza el saldo del monedero según el tipo de operación y persiste la operación.
     *
     * @param operation operación completada
     * @return {@code Completable} que notifica la finalización
     */
    private Completable applyBalanceAndPersist(WalletOperation operation) {
        return Maybe.fromPublisher(walletRepository.findById(operation.getWalletId()))
                .switchIfEmpty(Maybe.<Wallet>error(
                        new IllegalStateException("Monedero no encontrado: " + operation.getWalletId())))
                .flatMapSingle(wallet -> {
                    double balance = wallet.getBalance() == null ? 0.0 : wallet.getBalance();
                    double amount = operation.getAmount() == null ? 0.0 : operation.getAmount();
                    if (WalletOperationType.TOP_UP.name().equals(operation.getOperationType())) {
                        wallet.setBalance(balance + amount);
                    } else {
                        wallet.setBalance(balance - amount);
                    }
                    return Single.fromPublisher(walletRepository.save(wallet));
                })
                .flatMapCompletable(saved -> persistOperation(operation));
    }

    /**
     * Persiste la operación con su estado final.
     *
     * @param operation operación a persistir
     * @return {@code Completable} que notifica la finalización
     */
    private Completable persistOperation(WalletOperation operation) {
        return Single.fromPublisher(walletOperationRepository.save(operation)).ignoreElement();
    }
}
