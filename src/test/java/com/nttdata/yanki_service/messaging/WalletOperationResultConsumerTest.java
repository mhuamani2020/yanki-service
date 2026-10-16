package com.nttdata.yanki_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nttdata.yanki_service.event.WalletOperationResult;
import com.nttdata.yanki_service.model.Wallet;
import com.nttdata.yanki_service.model.WalletOperation;
import com.nttdata.yanki_service.repository.WalletOperationRepository;
import com.nttdata.yanki_service.repository.WalletRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Pruebas unitarias de {@link WalletOperationResultConsumer} con sus repositorios mockeados
 * (Parte III: los nuevos métodos públicos deben tener sus pruebas con mocks).
 * <p>
 * Se verifica que, al recibir el resultado de bank-account-service, se actualice el saldo del
 * monedero (según {@code TOP_UP} abona o {@code WITHDRAW} debita) y se cierre la operación;
 * que un resultado {@code FAILED} no altere el saldo; y que los mensajes inválidos se
 * descarten sin efectos.
 *
 * @author NTT Data
 */
class WalletOperationResultConsumerTest {

    private WalletOperationRepository walletOperationRepository;
    private WalletRepository walletRepository;
    private WalletOperationResultConsumer consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        walletOperationRepository = mock(WalletOperationRepository.class);
        walletRepository = mock(WalletRepository.class);
        consumer = new WalletOperationResultConsumer(walletOperationRepository, walletRepository, objectMapper);
    }

    @Test
    @DisplayName("TOP_UP COMPLETED: abona el importe al monedero y cierra la operación")
    void topUpCompleted_shouldIncreaseWalletBalance() {
        WalletOperation operation = WalletOperation.builder()
                .id("op-1").walletId("w-1").operationType("TOP_UP")
                .amount(100.0).status("PENDING").build();
        Wallet wallet = Wallet.builder().id("w-1").balance(50.0).build();

        when(walletOperationRepository.findById("op-1")).thenReturn(Mono.just(operation));
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(wallet));
        when(walletRepository.save(any(Wallet.class))).thenReturn(Mono.just(wallet));
        when(walletOperationRepository.save(any(WalletOperation.class))).thenReturn(Mono.just(operation));

        consumer.onWalletOperationResult(json("op-1", "COMPLETED", "tx-1", 400.0, null));

        ArgumentCaptor<Wallet> walletCaptor = ArgumentCaptor.forClass(Wallet.class);
        verify(walletRepository).save(walletCaptor.capture());
        assertThat(walletCaptor.getValue().getBalance()).isEqualTo(150.0);

        ArgumentCaptor<WalletOperation> opCaptor = ArgumentCaptor.forClass(WalletOperation.class);
        verify(walletOperationRepository).save(opCaptor.capture());
        assertThat(opCaptor.getValue().getStatus()).isEqualTo("COMPLETED");
        assertThat(opCaptor.getValue().getTransactionId()).isEqualTo("tx-1");
    }

    @Test
    @DisplayName("WITHDRAW COMPLETED: debita el importe del monedero y cierra la operación")
    void withdrawCompleted_shouldDecreaseWalletBalance() {
        WalletOperation operation = WalletOperation.builder()
                .id("op-2").walletId("w-1").operationType("WITHDRAW")
                .amount(30.0).status("PENDING").build();
        Wallet wallet = Wallet.builder().id("w-1").balance(200.0).build();

        when(walletOperationRepository.findById("op-2")).thenReturn(Mono.just(operation));
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(wallet));
        when(walletRepository.save(any(Wallet.class))).thenReturn(Mono.just(wallet));
        when(walletOperationRepository.save(any(WalletOperation.class))).thenReturn(Mono.just(operation));

        consumer.onWalletOperationResult(json("op-2", "COMPLETED", "tx-2", 900.0, null));

        ArgumentCaptor<Wallet> walletCaptor = ArgumentCaptor.forClass(Wallet.class);
        verify(walletRepository).save(walletCaptor.capture());
        assertThat(walletCaptor.getValue().getBalance()).isEqualTo(170.0);
    }

    @Test
    @DisplayName("FAILED: marca la operación como fallida sin alterar el saldo")
    void failedResult_shouldNotTouchWalletBalance() {
        WalletOperation operation = WalletOperation.builder()
                .id("op-3").walletId("w-1").operationType("TOP_UP")
                .amount(100.0).status("PENDING").build();

        when(walletOperationRepository.findById("op-3")).thenReturn(Mono.just(operation));
        when(walletOperationRepository.save(any(WalletOperation.class))).thenReturn(Mono.just(operation));

        consumer.onWalletOperationResult(json("op-3", "FAILED", null, null, "Saldo insuficiente"));

        ArgumentCaptor<WalletOperation> opCaptor = ArgumentCaptor.forClass(WalletOperation.class);
        verify(walletOperationRepository).save(opCaptor.capture());
        assertThat(opCaptor.getValue().getStatus()).isEqualTo("FAILED");
        assertThat(opCaptor.getValue().getErrorMessage()).isEqualTo("Saldo insuficiente");

        verify(walletRepository, never()).save(any(Wallet.class));
        verifyNoInteractions(walletRepository);
    }

    @Test
    @DisplayName("Operación desconocida: no lanza excepción ni toca el monedero")
    void unknownOperation_shouldNotThrow() {
        when(walletOperationRepository.findById("op-x")).thenReturn(Mono.empty());

        consumer.onWalletOperationResult(json("op-x", "COMPLETED", "tx", 1.0, null));

        verifyNoInteractions(walletRepository);
        verify(walletOperationRepository, never()).save(any(WalletOperation.class));
    }

    @Test
    @DisplayName("JSON inválido: se descarta sin tocar los repositorios")
    void invalidJson_shouldBeDiscarded() {
        consumer.onWalletOperationResult("{no-es-json}");

        verifyNoInteractions(walletOperationRepository);
        verifyNoInteractions(walletRepository);
    }

    /**
     * Serializa un resultado de operación de monedero a JSON.
     *
     * @param operationId  identificador de la operación
     * @param status       estado del resultado
     * @param transactionId movimiento generado en la cuenta
     * @param balanceAfter saldo de la cuenta tras la operación
     * @param errorMessage mensaje de error (si aplica)
     * @return JSON del resultado
     */
    private String json(String operationId, String status, String transactionId, Double balanceAfter,
                        String errorMessage) {
        WalletOperationResult result = WalletOperationResult.builder()
                .operationId(operationId)
                .status(status)
                .transactionId(transactionId)
                .balanceAfter(balanceAfter)
                .errorMessage(errorMessage)
                .processedAt("2026-01-01T00:00:00Z")
                .build();
        try {
            return objectMapper.writeValueAsString(result);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
