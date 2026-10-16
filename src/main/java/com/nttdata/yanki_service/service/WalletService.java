package com.nttdata.yanki_service.service;

import com.nttdata.yanki_service.dto.AssociateDebitCardRequest;
import com.nttdata.yanki_service.dto.WalletAmountRequest;
import com.nttdata.yanki_service.dto.WalletOperationResponse;
import com.nttdata.yanki_service.dto.WalletRequest;
import com.nttdata.yanki_service.dto.WalletResponse;
import com.nttdata.yanki_service.dto.WalletTransferRequest;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

/**
 * Contrato de negocio del monedero móvil Yanki (Parte III).
 * <p>
 * Las <b>nuevas funcionalidades</b> se implementan con el modelo de reactividad de
 * <b>RxJava 3</b> ({@link Single} / {@link Maybe} / {@link Flowable}); el controlador los
 * adapta a Reactor en el borde HTTP.
 *
 * @author NTT Data
 */
public interface WalletService {

    /**
     * Obtiene todos los monederos.
     *
     * @return flujo con los monederos en formato de respuesta
     */
    Flowable<WalletResponse> findAll();

    /**
     * Obtiene un monedero por su identificador.
     *
     * @param id identificador del monedero
     * @return maybe con el monedero encontrado; vacío si no existe
     */
    Maybe<WalletResponse> findById(String id);

    /**
     * Obtiene un monedero por su número de celular.
     *
     * @param cellphone número de celular del monedero
     * @return maybe con el monedero encontrado; vacío si no existe
     */
    Maybe<WalletResponse> findByCellphone(String cellphone);

    /**
     * Registra un nuevo monedero.
     *
     * @param request datos del monedero a crear
     * @return single con el monedero recién creado
     */
    Single<WalletResponse> create(WalletRequest request);

    /**
     * Asocia una tarjeta de débito del banco al monedero.
     *
     * @param id      identificador del monedero
     * @param request tarjeta de débito a asociar
     * @return maybe con el monedero actualizado; vacío si no existe
     */
    Maybe<WalletResponse> associateDebitCard(String id, AssociateDebitCardRequest request);

    /**
     * Envía un pago del monedero indicado a otro monedero identificado por su celular.
     *
     * @param id      identificador del monedero origen
     * @param request celular destino e importe
     * @return single con el monedero origen actualizado
     */
    Single<WalletResponse> transfer(String id, WalletTransferRequest request);

    /**
     * Carga saldo al monedero debitando la cuenta de la tarjeta de débito asociada.
     * <p>
     * La operación es asíncrona: se publica en Kafka y el saldo se actualiza al recibir el
     * resultado por {@code wallet-operation-results}.
     *
     * @param id      identificador del monedero
     * @param request importe a cargar
     * @return single con la operación creada en estado {@code PENDING}
     */
    Single<WalletOperationResponse> topUp(String id, WalletAmountRequest request);

    /**
     * Acredita el saldo del monedero en la cuenta de la tarjeta de débito asociada.
     * <p>
     * La operación es asíncrona: se publica en Kafka y el saldo se actualiza al recibir el
     * resultado por {@code wallet-operation-results}.
     *
     * @param id      identificador del monedero
     * @param request importe a acreditar
     * @return single con la operación creada en estado {@code PENDING}
     */
    Single<WalletOperationResponse> withdraw(String id, WalletAmountRequest request);
}
