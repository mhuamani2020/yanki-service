package com.nttdata.yanki_service.controller;

import com.nttdata.commons.resilience.error.StandardApiExceptionHandler;
import com.nttdata.yanki_service.config.TestSecurityConfig;
import com.nttdata.yanki_service.dto.WalletOperationResponse;
import com.nttdata.yanki_service.dto.WalletResponse;
import com.nttdata.yanki_service.service.WalletService;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Pruebas del controlador del monedero Yanki, que implementa la interfaz {@code WalletsApi}
 * generada desde el contrato OpenAPI (<i>contract-first</i>).
 * <p>
 * El servicio se mockea devolviendo tipos de <b>RxJava 3</b>; se verifica que el controlador
 * los adapta a Reactor y traduce los resultados a los códigos HTTP esperados (201 creación,
 * 200 consulta, 202 operación asíncrona, 404 no encontrado, 400 dato inválido).
 *
 * @author NTT Data
 */
@WebFluxTest(controllers = WalletController.class)
@Import({TestSecurityConfig.class, StandardApiExceptionHandler.class})
class WalletControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private WalletService walletService;

    @Test
    @DisplayName("POST /wallets: Retorna 201 con el monedero creado")
    void createWallet_shouldReturnCreated() {
        WalletResponse response = new WalletResponse();
        response.setId("w-1");
        response.setCellphone("999888777");
        response.setBalance(0.0);
        when(walletService.create(any())).thenReturn(Single.just(response));

        webTestClient.post()
                .uri("/wallets")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"documentNumber\":\"123\",\"cellphone\":\"999888777\","
                        + "\"imei\":\"IMEI-1\",\"email\":\"u@mail.com\"}")
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.id").isEqualTo("w-1")
                .jsonPath("$.cellphone").isEqualTo("999888777");
    }

    @Test
    @DisplayName("POST /wallets: Retorna 400 si los datos son inválidos")
    void createWallet_whenInvalid_shouldReturnBadRequest() {
        when(walletService.create(any()))
                .thenReturn(Single.error(new IllegalArgumentException("Datos obligatorios")));

        webTestClient.post()
                .uri("/wallets")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"documentNumber\":\"123\",\"cellphone\":\"999888777\","
                        + "\"imei\":\"IMEI-1\",\"email\":\"u@mail.com\"}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Datos obligatorios");
    }

    @Test
    @DisplayName("PUT /wallets/{id}: Retorna 200 con el monedero actualizado")
    void updateWallet_shouldReturnUpdated() {
        WalletResponse response = new WalletResponse();
        response.setId("w-1");
        response.setEmail("new@mail.com");
        when(walletService.update(eq("w-1"), any())).thenReturn(Maybe.just(response));

        webTestClient.put()
                .uri("/wallets/w-1")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"documentNumber\":\"123\",\"cellphone\":\"999888777\","
                        + "\"imei\":\"IMEI-1\",\"email\":\"new@mail.com\"}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.email").isEqualTo("new@mail.com");
    }

    @Test
    @DisplayName("PUT /wallets/{id}: Retorna 404 si no existe")
    void updateWallet_whenMissing_shouldReturnNotFound() {
        when(walletService.update(eq("nope"), any())).thenReturn(Maybe.empty());

        webTestClient.put()
                .uri("/wallets/nope")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"documentNumber\":\"123\",\"cellphone\":\"999888777\","
                        + "\"imei\":\"IMEI-1\",\"email\":\"u@mail.com\"}")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("PUT /wallets/{id}: Retorna 400 si los datos son inválidos")
    void updateWallet_whenInvalid_shouldReturnBadRequest() {
        when(walletService.update(eq("w-1"), any()))
                .thenReturn(Maybe.error(new IllegalArgumentException("Datos obligatorios")));

        webTestClient.put()
                .uri("/wallets/w-1")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"cellphone\":\"999888777\"}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("DELETE /wallets/{id}: Retorna 204 si se elimina")
    void deleteWallet_shouldReturnNoContent() {
        when(walletService.delete("w-1")).thenReturn(Single.just(true));

        webTestClient.delete()
                .uri("/wallets/w-1")
                .exchange()
                .expectStatus().isNoContent();
    }

    @Test
    @DisplayName("DELETE /wallets/{id}: Retorna 404 si no existe")
    void deleteWallet_whenMissing_shouldReturnNotFound() {
        when(walletService.delete("nope")).thenReturn(Single.just(false));

        webTestClient.delete()
                .uri("/wallets/nope")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("GET /wallets/{id}: Retorna 200 con el monedero")
    void findWalletById_shouldReturnWallet() {
        WalletResponse response = new WalletResponse();
        response.setId("w-1");
        when(walletService.findById("w-1")).thenReturn(Maybe.just(response));

        webTestClient.get()
                .uri("/wallets/w-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo("w-1");
    }

    @Test
    @DisplayName("GET /wallets/{id}: Retorna 404 si no existe")
    void findWalletById_whenMissing_shouldReturnNotFound() {
        when(walletService.findById("nope")).thenReturn(Maybe.empty());

        webTestClient.get()
                .uri("/wallets/nope")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("GET /wallets/by-cellphone/{cellphone}: Retorna 200 con el monedero")
    void findWalletByCellphone_shouldReturnWallet() {
        WalletResponse response = new WalletResponse();
        response.setCellphone("999888777");
        when(walletService.findByCellphone("999888777")).thenReturn(Maybe.just(response));

        webTestClient.get()
                .uri("/wallets/by-cellphone/999888777")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.cellphone").isEqualTo("999888777");
    }

    @Test
    @DisplayName("POST /wallets/{id}/debit-card: Retorna 200 con la tarjeta asociada")
    void associateDebitCard_shouldReturnWallet() {
        WalletResponse response = new WalletResponse();
        response.setId("w-1");
        response.setDebitCardId("card-01");
        when(walletService.associateDebitCard(eq("w-1"), any())).thenReturn(Maybe.just(response));

        webTestClient.post()
                .uri("/wallets/w-1/debit-card")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"debitCardId\":\"card-01\"}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.debitCardId").isEqualTo("card-01");
    }

    @Test
    @DisplayName("POST /wallets/{id}/payments: Retorna 200 con el monedero origen")
    void transferBetweenWallets_shouldReturnSourceWallet() {
        WalletResponse response = new WalletResponse();
        response.setId("w-1");
        response.setBalance(70.0);
        when(walletService.transfer(eq("w-1"), any())).thenReturn(Single.just(response));

        webTestClient.post()
                .uri("/wallets/w-1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"targetCellphone\":\"999111222\",\"amount\":30.0}")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.balance").isEqualTo(70.0);
    }

    @Test
    @DisplayName("POST /wallets/{id}/top-up: Retorna 202 con la operación pendiente")
    void topUpWallet_shouldReturnAccepted() {
        WalletOperationResponse response = new WalletOperationResponse();
        response.setOperationId("op-1");
        response.setOperationType("TOP_UP");
        response.setStatus("PENDING");
        when(walletService.topUp(eq("w-1"), any())).thenReturn(Single.just(response));

        webTestClient.post()
                .uri("/wallets/w-1/top-up")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"amount\":100.0}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.operationId").isEqualTo("op-1")
                .jsonPath("$.status").isEqualTo("PENDING");
    }

    @Test
    @DisplayName("POST /wallets/{id}/withdraw: Retorna 202 con la operación pendiente")
    void withdrawFromWallet_shouldReturnAccepted() {
        WalletOperationResponse response = new WalletOperationResponse();
        response.setOperationId("op-2");
        response.setOperationType("WITHDRAW");
        response.setStatus("PENDING");
        when(walletService.withdraw(eq("w-1"), any())).thenReturn(Single.just(response));

        webTestClient.post()
                .uri("/wallets/w-1/withdraw")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"amount\":50.0}")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.operationType").isEqualTo("WITHDRAW");
    }
}
