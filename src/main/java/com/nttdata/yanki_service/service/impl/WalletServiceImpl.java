package com.nttdata.yanki_service.service.impl;

import com.nttdata.yanki_service.dto.AssociateDebitCardRequest;
import com.nttdata.yanki_service.dto.WalletAmountRequest;
import com.nttdata.yanki_service.dto.WalletOperationResponse;
import com.nttdata.yanki_service.dto.WalletRequest;
import com.nttdata.yanki_service.dto.WalletResponse;
import com.nttdata.yanki_service.dto.WalletTransferRequest;
import com.nttdata.yanki_service.enums.WalletOperationStatus;
import com.nttdata.yanki_service.enums.WalletOperationType;
import com.nttdata.yanki_service.event.WalletOperationEvent;
import com.nttdata.yanki_service.mapper.WalletMapper;
import com.nttdata.yanki_service.messaging.KafkaEventPublisher;
import com.nttdata.yanki_service.model.Wallet;
import com.nttdata.yanki_service.model.WalletOperation;
import com.nttdata.yanki_service.repository.WalletOperationRepository;
import com.nttdata.yanki_service.repository.WalletRepository;
import com.nttdata.yanki_service.service.WalletService;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Implementación del contrato {@link WalletService} con el modelo de reactividad de
 * <b>RxJava 3</b> (Parte III).
 * <p>
 * Reglas aplicadas:
 * <ul>
 *     <li>el registro no exige ser cliente del banco, pero el número de celular es único;</li>
 *     <li>las transferencias entre monederos se resuelven por número de celular y exigen
 *     saldo suficiente en el origen;</li>
 *     <li>la carga y el retiro de saldo contra la tarjeta de débito se delegan a
 *     {@code bank-account-service} <b>por Kafka</b> (nunca por REST): se crea una operación
 *     {@code PENDING} y se publica el comando; el saldo del monedero se actualiza al recibir
 *     el resultado en {@code wallet-operation-results}.</li>
 * </ul>
 * Los repositorios reactivos (Reactor) se adaptan al borde con {@code fromPublisher}. La
 * ausencia del recurso se modela con {@link Maybe} (que admite el caso vacío) y se convierte
 * a {@link Single} una vez garantizada su presencia.
 *
 * @author NTT Data
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WalletServiceImpl implements WalletService {

    private final WalletRepository walletRepository;
    private final WalletOperationRepository walletOperationRepository;
    private final KafkaEventPublisher eventPublisher;

    @Override
    public Flowable<WalletResponse> findAll() {
        return Flowable.fromPublisher(walletRepository.findAll())
                .map(WalletMapper::toResponse);
    }

    @Override
    public Maybe<WalletResponse> findById(String id) {
        return Maybe.fromPublisher(walletRepository.findById(id))
                .map(WalletMapper::toResponse);
    }

    @Override
    public Maybe<WalletResponse> findByCellphone(String cellphone) {
        return Maybe.fromPublisher(walletRepository.findByCellphone(cellphone))
                .map(WalletMapper::toResponse);
    }

    @Override
    public Single<WalletResponse> create(WalletRequest request) {
        if (request == null || isBlank(request.getDocumentNumber()) || isBlank(request.getCellphone())
                || isBlank(request.getImei()) || isBlank(request.getEmail())) {
            return Single.error(new IllegalArgumentException(
                    "Documento, celular, IMEI y correo electrónico son obligatorios"));
        }

        return Single.fromPublisher(walletRepository.existsByCellphone(request.getCellphone()))
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return Single.error(new IllegalArgumentException(
                                "Ya existe un monedero con ese número de celular"));
                    }
                    Wallet wallet = Wallet.builder()
                            .documentNumber(request.getDocumentNumber())
                            .cellphone(request.getCellphone())
                            .imei(request.getImei())
                            .email(request.getEmail())
                            .balance(0.0)
                            .build();
                    log.info("Registering wallet for cellphone {}", request.getCellphone());
                    return Single.fromPublisher(walletRepository.save(wallet));
                })
                .map(WalletMapper::toResponse);
    }

    @Override
    public Maybe<WalletResponse> update(String id, WalletRequest request) {
        if (request == null || isBlank(request.getDocumentNumber()) || isBlank(request.getCellphone())
                || isBlank(request.getImei()) || isBlank(request.getEmail())) {
            return Maybe.error(new IllegalArgumentException(
                    "Documento, celular, IMEI y correo electrónico son obligatorios"));
        }

        return Maybe.fromPublisher(walletRepository.findById(id))
                .flatMap(wallet -> isCellphoneTakenByAnotherWallet(wallet, request.getCellphone())
                        .flatMap(taken -> {
                            if (Boolean.TRUE.equals(taken)) {
                                return Maybe.error(new IllegalArgumentException(
                                        "Ya existe un monedero con ese número de celular"));
                            }
                            wallet.setDocumentNumber(request.getDocumentNumber());
                            wallet.setCellphone(request.getCellphone());
                            wallet.setImei(request.getImei());
                            wallet.setEmail(request.getEmail());
                            log.info("Updating wallet {}", wallet.getId());
                            return Maybe.fromPublisher(walletRepository.save(wallet));
                        }))
                .map(WalletMapper::toResponse);
    }

    @Override
    public Single<Boolean> delete(String id) {
        return Single.fromPublisher(walletRepository.existsById(id))
                .flatMap(exists -> {
                    if (Boolean.FALSE.equals(exists)) {
                        return Single.just(false);
                    }
                    log.info("Deleting wallet {}", id);
                    return Single.fromPublisher(walletRepository.deleteById(id).thenReturn(true));
                });
    }

    /**
     * Comprueba si el nuevo celular pertenece a un monedero distinto del que se actualiza.
     *
     * @param wallet       monedero que se está actualizando
     * @param newCellphone nuevo número de celular
     * @return maybe con {@code true} si otro monedero ya usa ese celular
     */
    private Maybe<Boolean> isCellphoneTakenByAnotherWallet(Wallet wallet, String newCellphone) {
        if (newCellphone.equals(wallet.getCellphone())) {
            return Maybe.just(false);
        }
        return Maybe.fromPublisher(walletRepository.existsByCellphone(newCellphone));
    }

    @Override
    public Maybe<WalletResponse> associateDebitCard(String id, AssociateDebitCardRequest request) {
        if (request == null || isBlank(request.getDebitCardId())) {
            return Maybe.error(new IllegalArgumentException("La tarjeta de débito es obligatoria"));
        }
        return Maybe.fromPublisher(walletRepository.findById(id))
                .flatMap(wallet -> {
                    wallet.setDebitCardId(request.getDebitCardId());
                    return Maybe.fromPublisher(walletRepository.save(wallet));
                })
                .map(WalletMapper::toResponse);
    }

    @Override
    public Single<WalletResponse> transfer(String id, WalletTransferRequest request) {
        if (request == null || isBlank(request.getTargetCellphone()) || request.getAmount() == null
                || request.getAmount() <= 0) {
            return Single.error(new IllegalArgumentException(
                    "El celular destino y un importe positivo son obligatorios"));
        }

        return requireWallet(id)
                .flatMap(source -> requireWalletByCellphone(request.getTargetCellphone())
                        .flatMapSingle(target -> applyTransfer(source, target, request.getAmount())))
                .map(WalletMapper::toResponse)
                .toSingle();
    }

    @Override
    public Single<WalletOperationResponse> topUp(String id, WalletAmountRequest request) {
        if (request == null || request.getAmount() == null || request.getAmount() <= 0) {
            return Single.error(new IllegalArgumentException("El importe debe ser mayor que cero"));
        }
        return requireWallet(id)
                .flatMapSingle(wallet -> createPendingOperation(wallet, WalletOperationType.TOP_UP,
                        request.getAmount()))
                .toSingle();
    }

    @Override
    public Single<WalletOperationResponse> withdraw(String id, WalletAmountRequest request) {
        if (request == null || request.getAmount() == null || request.getAmount() <= 0) {
            return Single.error(new IllegalArgumentException("El importe debe ser mayor que cero"));
        }
        return requireWallet(id)
                .flatMapSingle(wallet -> createPendingOperation(wallet, WalletOperationType.WITHDRAW,
                        request.getAmount()))
                .toSingle();
    }

    /**
     * Recupera un monedero existente o falla con error de negocio si no existe.
     *
     * @param id identificador del monedero
     * @return maybe con el monedero; error si no existe
     */
    private Maybe<Wallet> requireWallet(String id) {
        return Maybe.fromPublisher(walletRepository.findById(id))
                .switchIfEmpty(Maybe.<Wallet>error(new IllegalArgumentException("El monedero no existe")));
    }

    /**
     * Recupera el monedero destino por su celular o falla con error de negocio si no existe.
     *
     * @param cellphone número de celular del monedero destino
     * @return maybe con el monedero; error si no existe
     */
    private Maybe<Wallet> requireWalletByCellphone(String cellphone) {
        return Maybe.fromPublisher(walletRepository.findByCellphone(cellphone))
                .switchIfEmpty(Maybe.<Wallet>error(
                        new IllegalArgumentException("El monedero destino no existe")));
    }

    /**
     * Aplica el movimiento de saldo entre los dos monederos y los persiste.
     *
     * @param source monedero origen
     * @param target monedero destino
     * @param amount importe a transferir
     * @return single con el monedero origen ya actualizado
     */
    private Single<Wallet> applyTransfer(Wallet source, Wallet target, Double amount) {
        if (source.getId().equals(target.getId())) {
            return Single.error(new IllegalArgumentException("No se puede transferir al mismo monedero"));
        }
        double sourceBalance = source.getBalance() == null ? 0.0 : source.getBalance();
        if (sourceBalance < amount) {
            return Single.error(new IllegalArgumentException("Saldo insuficiente en el monedero"));
        }
        double targetBalance = target.getBalance() == null ? 0.0 : target.getBalance();
        source.setBalance(sourceBalance - amount);
        target.setBalance(targetBalance + amount);

        return Single.fromPublisher(walletRepository.save(source)
                .then(walletRepository.save(target))
                .thenReturn(source));
    }

    /**
     * Crea la operación {@code PENDING}, la persiste y publica el comando en Kafka.
     *
     * @param wallet monedero afectado
     * @param type   tipo de operación
     * @param amount importe
     * @return single con la operación creada
     */
    private Single<WalletOperationResponse> createPendingOperation(Wallet wallet, WalletOperationType type,
                                                                   Double amount) {
        if (isBlank(wallet.getDebitCardId())) {
            return Single.error(new IllegalArgumentException(
                    "El monedero no tiene una tarjeta de débito asociada"));
        }
        double balance = wallet.getBalance() == null ? 0.0 : wallet.getBalance();
        if (type == WalletOperationType.WITHDRAW && balance < amount) {
            return Single.error(new IllegalArgumentException("Saldo insuficiente en el monedero"));
        }

        WalletOperation operation = WalletOperation.builder()
                .id(UUID.randomUUID().toString())
                .walletId(wallet.getId())
                .operationType(type.name())
                .amount(amount)
                .status(WalletOperationStatus.PENDING.name())
                .debitCardId(wallet.getDebitCardId())
                .createdAt(LocalDateTime.now())
                .build();

        WalletOperationEvent event = WalletOperationEvent.builder()
                .operationId(operation.getId())
                .operationType(type.name())
                .debitCardId(operation.getDebitCardId())
                .amount(amount)
                .requestedAt(Instant.now().toString())
                .build();

        return Single.fromPublisher(walletOperationRepository.save(operation))
                .flatMap(saved -> eventPublisher.publishWalletOperation(event)
                        .andThen(Single.just(saved)))
                .map(WalletMapper::toResponse);
    }

    /**
     * Comprueba si una cadena es nula o está vacía.
     *
     * @param value cadena a comprobar
     * @return {@code true} si es nula o vacía
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
