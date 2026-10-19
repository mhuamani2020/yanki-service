package com.nttdata.yanki_service.controller;

import com.nttdata.yanki_service.api.WalletsApi;
import com.nttdata.yanki_service.dto.AssociateDebitCardRequest;
import com.nttdata.yanki_service.dto.WalletAmountRequest;
import com.nttdata.yanki_service.dto.WalletOperationResponse;
import com.nttdata.yanki_service.dto.WalletRequest;
import com.nttdata.yanki_service.dto.WalletResponse;
import com.nttdata.yanki_service.dto.WalletTransferRequest;
import com.nttdata.yanki_service.service.WalletService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Controlador REST del monedero móvil Yanki (Parte III).
 * <p>
 * Implementa la interfaz {@link WalletsApi} generada a partir del contrato OpenAPI
 * ({@code src/main/resources/openapi.yaml}); así el contrato y la implementación no pueden
 * divergir (<i>contract-first</i>).
 * <p>
 * La reactividad de las <b>nuevas funcionalidades</b> se implementa con <b>RxJava 3</b> en la
 * capa de servicio ({@link WalletService} devuelve {@code Single}/{@code Maybe}/{@code
 * Flowable}); en el borde HTTP esos tipos se adaptan a Reactor (tipo declarado por la interfaz
 * generada) con {@code Mono.from(...toFlowable())} y {@code Flux.from(...)}.
 * <p>
 * Las operaciones de carga y retiro de saldo son asíncronas: devuelven HTTP 202 con la
 * operación en estado {@code PENDING} y el resultado se procesa por Kafka.
 *
 * @author NTT Data
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class WalletController implements WalletsApi {

    private final WalletService walletService;

    /**
     * {@code GET /wallets} — lista todos los monederos.
     *
     * @param exchange contexto de la petición
     * @return respuesta 200 con el flujo de monederos
     */
    @Override
    public Mono<ResponseEntity<Flux<WalletResponse>>> findAllWallets(ServerWebExchange exchange) {
        log.info("Fetching all wallets");
        return Mono.just(ResponseEntity.ok(Flux.from(walletService.findAll())));
    }

    /**
     * {@code POST /wallets} — registra un monedero.
     *
     * @param walletRequest datos del monedero
     * @param exchange      contexto de la petición
     * @return respuesta 201 con el monedero creado
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> createWallet(Mono<WalletRequest> walletRequest,
                                                             ServerWebExchange exchange) {
        log.info("Creating wallet");
        return walletRequest
                .flatMap(request -> Mono.from(walletService.create(request).toFlowable()))
                .map(response -> ResponseEntity.status(HttpStatus.CREATED).body(response));
    }

    /**
     * {@code PUT /wallets/{id}} — actualiza los datos maestros de un monedero.
     *
     * @param id            identificador del monedero
     * @param walletRequest nuevos datos del monedero
     * @param exchange      contexto de la petición
     * @return respuesta 200 con el monedero actualizado, o 404 si no existe
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> updateWallet(String id, Mono<WalletRequest> walletRequest,
                                                             ServerWebExchange exchange) {
        log.info("Updating wallet: {}", id);
        return walletRequest
                .flatMap(request -> Mono.from(walletService.update(id, request).toFlowable()))
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * {@code DELETE /wallets/{id}} — elimina un monedero.
     *
     * @param id       identificador del monedero
     * @param exchange contexto de la petición
     * @return respuesta 204 si se eliminó, o 404 si no existe
     */
    @Override
    public Mono<ResponseEntity<Void>> deleteWallet(String id, ServerWebExchange exchange) {
        log.info("Deleting wallet: {}", id);
        return Mono.from(walletService.delete(id).toFlowable())
                .flatMap(deleted -> Boolean.TRUE.equals(deleted)
                        ? Mono.just(ResponseEntity.<Void>noContent().build())
                        : Mono.just(ResponseEntity.<Void>notFound().build()));
    }

    /**
     * {@code GET /wallets/{id}} — consulta un monedero por su id.
     *
     * @param id       identificador del monedero
     * @param exchange contexto de la petición
     * @return respuesta 200 con el monedero, o 404 si no existe
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> findWalletById(String id, ServerWebExchange exchange) {
        log.info("Fetching wallet by id: {}", id);
        return Mono.from(walletService.findById(id).toFlowable())
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * {@code GET /wallets/by-cellphone/{cellphone}} — consulta un monedero por su celular.
     *
     * @param cellphone número de celular del monedero
     * @param exchange  contexto de la petición
     * @return respuesta 200 con el monedero, o 404 si no existe
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> findWalletByCellphone(String cellphone,
                                                                      ServerWebExchange exchange) {
        log.info("Fetching wallet by cellphone: {}", cellphone);
        return Mono.from(walletService.findByCellphone(cellphone).toFlowable())
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * {@code POST /wallets/{id}/debit-card} — asocia una tarjeta de débito al monedero.
     *
     * @param id                        identificador del monedero
     * @param associateDebitCardRequest tarjeta de débito a asociar
     * @param exchange                  contexto de la petición
     * @return respuesta 200 con el monedero actualizado, o 404 si no existe
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> associateDebitCard(
            String id, Mono<AssociateDebitCardRequest> associateDebitCardRequest, ServerWebExchange exchange) {
        log.info("Associating debit card to wallet: {}", id);
        return associateDebitCardRequest
                .flatMap(request -> Mono.from(walletService.associateDebitCard(id, request).toFlowable()))
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * {@code POST /wallets/{id}/payments} — envía un pago a otro monedero por su celular.
     *
     * @param id                    identificador del monedero origen
     * @param walletTransferRequest celular destino e importe
     * @param exchange              contexto de la petición
     * @return respuesta 200 con el monedero origen actualizado
     */
    @Override
    public Mono<ResponseEntity<WalletResponse>> transferBetweenWallets(
            String id, Mono<WalletTransferRequest> walletTransferRequest, ServerWebExchange exchange) {
        log.info("Transferring between wallets from: {}", id);
        return walletTransferRequest
                .flatMap(request -> Mono.from(walletService.transfer(id, request).toFlowable()))
                .map(ResponseEntity::ok);
    }

    /**
     * {@code POST /wallets/{id}/top-up} — carga saldo desde la tarjeta asociada (asíncrono).
     *
     * @param id                  identificador del monedero
     * @param walletAmountRequest importe a cargar
     * @param exchange            contexto de la petición
     * @return respuesta 202 con la operación pendiente
     */
    @Override
    public Mono<ResponseEntity<WalletOperationResponse>> topUpWallet(
            String id, Mono<WalletAmountRequest> walletAmountRequest, ServerWebExchange exchange) {
        log.info("Top-up request for wallet: {}", id);
        return walletAmountRequest
                .flatMap(request -> Mono.from(walletService.topUp(id, request).toFlowable()))
                .map(response -> ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
    }

    /**
     * {@code POST /wallets/{id}/withdraw} — acredita saldo en la tarjeta asociada (asíncrono).
     *
     * @param id                  identificador del monedero
     * @param walletAmountRequest importe a acreditar
     * @param exchange            contexto de la petición
     * @return respuesta 202 con la operación pendiente
     */
    @Override
    public Mono<ResponseEntity<WalletOperationResponse>> withdrawFromWallet(
            String id, Mono<WalletAmountRequest> walletAmountRequest, ServerWebExchange exchange) {
        log.info("Withdraw request for wallet: {}", id);
        return walletAmountRequest
                .flatMap(request -> Mono.from(walletService.withdraw(id, request).toFlowable()))
                .map(response -> ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
    }
}
