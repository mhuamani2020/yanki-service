package com.nttdata.yanki_service.service;

import com.nttdata.yanki_service.dto.AssociateDebitCardRequest;
import com.nttdata.yanki_service.dto.WalletAmountRequest;
import com.nttdata.yanki_service.dto.WalletRequest;
import com.nttdata.yanki_service.dto.WalletTransferRequest;
import com.nttdata.yanki_service.event.WalletOperationEvent;
import com.nttdata.yanki_service.messaging.KafkaEventPublisher;
import com.nttdata.yanki_service.model.Wallet;
import com.nttdata.yanki_service.model.WalletOperation;
import com.nttdata.yanki_service.repository.WalletOperationRepository;
import com.nttdata.yanki_service.repository.WalletRepository;
import com.nttdata.yanki_service.service.impl.WalletServiceImpl;
import io.reactivex.rxjava3.core.Completable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pruebas unitarias de {@link WalletServiceImpl} (Parte III) con los repositorios y el
 * publicador de Kafka mockeados.
 * <p>
 * Cubren el registro de monederos, las consultas, la asociación de tarjeta de débito, las
 * transferencias por celular y la creación de operaciones asíncronas de carga/retiro
 * ({@code PENDING}) publicadas en Kafka.
 *
 * @author NTT Data
 */
@ExtendWith(MockitoExtension.class)
class WalletServiceImplTest {

    @Mock
    private WalletRepository walletRepository;

    @Mock
    private WalletOperationRepository walletOperationRepository;

    @Mock
    private KafkaEventPublisher eventPublisher;

    @InjectMocks
    private WalletServiceImpl walletService;

    private Wallet sampleWallet;

    @BeforeEach
    void setUp() {
        sampleWallet = Wallet.builder()
                .id("w-1")
                .documentNumber("12345678")
                .cellphone("999888777")
                .imei("IMEI-1")
                .email("user@mail.com")
                .balance(100.0)
                .build();

        // La persistencia se comporta como un eco del documento guardado.
        lenient().when(walletRepository.save(any(Wallet.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        lenient().when(walletOperationRepository.save(any(WalletOperation.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        lenient().when(eventPublisher.publishWalletOperation(any(WalletOperationEvent.class)))
                .thenReturn(Completable.complete());
    }

    // --- CONSULTAS ---

    @Test
    @DisplayName("findAll: mapea todos los monederos")
    void findAll_shouldMapAll() {
        when(walletRepository.findAll()).thenReturn(Flux.just(sampleWallet,
                Wallet.builder().id("w-2").cellphone("999111222").build()));

        walletService.findAll().test().assertValueCount(2).assertComplete();
    }

    @Test
    @DisplayName("findById: devuelve el monedero mapeado")
    void findById_shouldMap() {
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        walletService.findById("w-1").test()
                .assertValue(response -> "w-1".equals(response.getId())
                        && "999888777".equals(response.getCellphone()))
                .assertComplete();
    }

    @Test
    @DisplayName("findByCellphone: devuelve el monedero mapeado")
    void findByCellphone_shouldMap() {
        when(walletRepository.findByCellphone("999888777")).thenReturn(Mono.just(sampleWallet));

        walletService.findByCellphone("999888777").test()
                .assertValue(response -> "w-1".equals(response.getId()))
                .assertComplete();
    }

    // --- REGISTRO ---

    @Test
    @DisplayName("create: registra el monedero con saldo inicial cero")
    void create_shouldRegisterWallet() {
        when(walletRepository.existsByCellphone("999888777")).thenReturn(Mono.just(false));

        WalletRequest request = new WalletRequest();
        request.setDocumentNumber("12345678");
        request.setCellphone("999888777");
        request.setImei("IMEI-1");
        request.setEmail("user@mail.com");

        walletService.create(request).test()
                .assertValue(response -> "999888777".equals(response.getCellphone())
                        && response.getBalance() == 0.0)
                .assertComplete();

        ArgumentCaptor<Wallet> captor = ArgumentCaptor.forClass(Wallet.class);
        verify(walletRepository).save(captor.capture());
        assertThat(captor.getValue().getBalance()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("create: falla si ya existe un monedero con ese celular")
    void create_whenDuplicateCellphone_shouldError() {
        when(walletRepository.existsByCellphone("999888777")).thenReturn(Mono.just(true));

        WalletRequest request = new WalletRequest();
        request.setDocumentNumber("12345678");
        request.setCellphone("999888777");
        request.setImei("IMEI-1");
        request.setEmail("user@mail.com");

        walletService.create(request).test()
                .assertError(IllegalArgumentException.class);
        verify(walletRepository, never()).save(any(Wallet.class));
    }

    @Test
    @DisplayName("create: falla si faltan datos obligatorios")
    void create_whenInvalidRequest_shouldError() {
        WalletRequest request = new WalletRequest();
        request.setCellphone("999888777");

        walletService.create(request).test()
                .assertError(IllegalArgumentException.class);
        verify(walletRepository, never()).save(any(Wallet.class));
    }

    // --- ACTUALIZACIÓN Y BORRADO (CRUD) ---

    @Test
    @DisplayName("update: actualiza los datos maestros sin alterar saldo ni tarjeta")
    void update_shouldModifyMasterData() {
        sampleWallet.setDebitCardId("card-01");
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        WalletRequest request = new WalletRequest();
        request.setDocumentNumber("87654321");
        request.setCellphone("999888777");
        request.setImei("IMEI-2");
        request.setEmail("new@mail.com");

        walletService.update("w-1", request).test()
                .assertValue(response -> "87654321".equals(response.getDocumentNumber())
                        && "IMEI-2".equals(response.getImei())
                        && "new@mail.com".equals(response.getEmail())
                        && response.getBalance() == 100.0
                        && "card-01".equals(response.getDebitCardId()))
                .assertComplete();

        verify(walletRepository).save(sampleWallet);
    }

    @Test
    @DisplayName("update: falla si el nuevo celular ya lo usa otro monedero")
    void update_whenCellphoneTaken_shouldError() {
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));
        when(walletRepository.existsByCellphone("999111222")).thenReturn(Mono.just(true));

        WalletRequest request = new WalletRequest();
        request.setDocumentNumber("12345678");
        request.setCellphone("999111222");
        request.setImei("IMEI-1");
        request.setEmail("user@mail.com");

        walletService.update("w-1", request).test()
                .assertError(IllegalArgumentException.class);
        verify(walletRepository, never()).save(any(Wallet.class));
    }

    @Test
    @DisplayName("update: no emite valor si el monedero no existe")
    void update_whenWalletMissing_shouldCompleteEmpty() {
        when(walletRepository.findById("w-9")).thenReturn(Mono.empty());

        WalletRequest request = new WalletRequest();
        request.setDocumentNumber("12345678");
        request.setCellphone("999888777");
        request.setImei("IMEI-1");
        request.setEmail("user@mail.com");

        walletService.update("w-9", request).test()
                .assertNoValues()
                .assertComplete();
    }

    @Test
    @DisplayName("update: falla si faltan datos obligatorios")
    void update_whenInvalidRequest_shouldError() {
        WalletRequest request = new WalletRequest();
        request.setCellphone("999888777");

        walletService.update("w-1", request).test()
                .assertError(IllegalArgumentException.class);
        verify(walletRepository, never()).save(any(Wallet.class));
    }

    @Test
    @DisplayName("delete: elimina el monedero y devuelve true")
    void delete_whenWalletExists_shouldReturnTrue() {
        when(walletRepository.existsById("w-1")).thenReturn(Mono.just(true));
        when(walletRepository.deleteById("w-1")).thenReturn(Mono.empty());

        walletService.delete("w-1").test()
                .assertValue(true)
                .assertComplete();

        verify(walletRepository).deleteById("w-1");
    }

    @Test
    @DisplayName("delete: devuelve false si el monedero no existe")
    void delete_whenWalletMissing_shouldReturnFalse() {
        when(walletRepository.existsById("w-9")).thenReturn(Mono.just(false));

        walletService.delete("w-9").test()
                .assertValue(false)
                .assertComplete();

        verify(walletRepository, never()).deleteById("w-9");
    }

    // --- TARJETA DE DÉBITO ---

    @Test
    @DisplayName("associateDebitCard: asocia la tarjeta y persiste el monedero")
    void associateDebitCard_shouldUpdateWallet() {
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        AssociateDebitCardRequest request = new AssociateDebitCardRequest();
        request.setDebitCardId("card-01");

        walletService.associateDebitCard("w-1", request).test()
                .assertValue(response -> "card-01".equals(response.getDebitCardId()))
                .assertComplete();
    }

    @Test
    @DisplayName("associateDebitCard: no emite valor si el monedero no existe")
    void associateDebitCard_whenWalletMissing_shouldCompleteEmpty() {
        when(walletRepository.findById("w-9")).thenReturn(Mono.empty());

        AssociateDebitCardRequest request = new AssociateDebitCardRequest();
        request.setDebitCardId("card-01");

        walletService.associateDebitCard("w-9", request).test()
                .assertNoValues()
                .assertComplete();
    }

    @Test
    @DisplayName("associateDebitCard: falla si no se indica la tarjeta")
    void associateDebitCard_whenRequestInvalid_shouldError() {
        walletService.associateDebitCard("w-1", new AssociateDebitCardRequest()).test()
                .assertError(IllegalArgumentException.class);
    }

    // --- TRANSFERENCIAS ENTRE MONEDEROS ---

    @Test
    @DisplayName("transfer: mueve el importe entre los dos monederos")
    void transfer_shouldMoveBalanceBetweenWallets() {
        Wallet target = Wallet.builder().id("w-2").cellphone("999111222").balance(10.0).build();
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));
        when(walletRepository.findByCellphone("999111222")).thenReturn(Mono.just(target));

        WalletTransferRequest request = new WalletTransferRequest();
        request.setTargetCellphone("999111222");
        request.setAmount(30.0);

        walletService.transfer("w-1", request).test()
                .assertValue(response -> response.getBalance() == 70.0)
                .assertComplete();

        verify(walletRepository, times(2)).save(any(Wallet.class));
        assertThat(sampleWallet.getBalance()).isEqualTo(70.0);
        assertThat(target.getBalance()).isEqualTo(40.0);
    }

    @Test
    @DisplayName("transfer: falla si el monedero origen no tiene saldo suficiente")
    void transfer_whenInsufficientBalance_shouldError() {
        Wallet target = Wallet.builder().id("w-2").cellphone("999111222").balance(10.0).build();
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));
        when(walletRepository.findByCellphone("999111222")).thenReturn(Mono.just(target));

        WalletTransferRequest request = new WalletTransferRequest();
        request.setTargetCellphone("999111222");
        request.setAmount(500.0);

        walletService.transfer("w-1", request).test()
                .assertError(IllegalArgumentException.class);
        verify(walletRepository, never()).save(any(Wallet.class));
    }

    @Test
    @DisplayName("transfer: falla si el monedero destino no existe")
    void transfer_whenTargetMissing_shouldError() {
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));
        when(walletRepository.findByCellphone("000")).thenReturn(Mono.empty());

        WalletTransferRequest request = new WalletTransferRequest();
        request.setTargetCellphone("000");
        request.setAmount(10.0);

        walletService.transfer("w-1", request).test()
                .assertError(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("transfer: falla si el celular destino corresponde al mismo monedero")
    void transfer_toSameWallet_shouldError() {
        Wallet target = Wallet.builder().id("w-1").cellphone("999888777").balance(100.0).build();
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));
        when(walletRepository.findByCellphone("999888777")).thenReturn(Mono.just(target));

        WalletTransferRequest request = new WalletTransferRequest();
        request.setTargetCellphone("999888777");
        request.setAmount(10.0);

        walletService.transfer("w-1", request).test()
                .assertError(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("transfer: falla si el importe no es positivo")
    void transfer_whenInvalidAmount_shouldError() {
        WalletTransferRequest request = new WalletTransferRequest();
        request.setTargetCellphone("999111222");
        request.setAmount(0.0);

        walletService.transfer("w-1", request).test()
                .assertError(IllegalArgumentException.class);
    }

    // --- CARGA (TOP_UP) ---

    @Test
    @DisplayName("topUp: crea la operación PENDING y publica el comando en Kafka")
    void topUp_shouldCreatePendingOperationAndPublish() {
        sampleWallet.setDebitCardId("card-01");
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(80.0);

        walletService.topUp("w-1", request).test()
                .assertValue(response -> "TOP_UP".equals(response.getOperationType())
                        && "PENDING".equals(response.getStatus())
                        && response.getAmount() == 80.0)
                .assertComplete();

        ArgumentCaptor<WalletOperationEvent> eventCaptor =
                ArgumentCaptor.forClass(WalletOperationEvent.class);
        verify(eventPublisher).publishWalletOperation(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getOperationType()).isEqualTo("TOP_UP");
        assertThat(eventCaptor.getValue().getDebitCardId()).isEqualTo("card-01");
        assertThat(eventCaptor.getValue().getAmount()).isEqualTo(80.0);
    }

    @Test
    @DisplayName("topUp: falla si el monedero no tiene tarjeta de débito asociada")
    void topUp_whenNoDebitCard_shouldError() {
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(80.0);

        walletService.topUp("w-1", request).test()
                .assertError(IllegalArgumentException.class);
        verify(eventPublisher, never()).publishWalletOperation(any(WalletOperationEvent.class));
    }

    @Test
    @DisplayName("topUp: falla si el monedero no existe")
    void topUp_whenWalletMissing_shouldError() {
        when(walletRepository.findById("w-9")).thenReturn(Mono.empty());

        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(80.0);

        walletService.topUp("w-9", request).test()
                .assertError(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("topUp: falla si el importe no es positivo")
    void topUp_whenInvalidAmount_shouldError() {
        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(-5.0);

        walletService.topUp("w-1", request).test()
                .assertError(IllegalArgumentException.class);
    }

    // --- RETIRO (WITHDRAW) ---

    @Test
    @DisplayName("withdraw: crea la operación PENDING y publica el comando en Kafka")
    void withdraw_shouldCreatePendingOperationAndPublish() {
        sampleWallet.setDebitCardId("card-01");
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(40.0);

        walletService.withdraw("w-1", request).test()
                .assertValue(response -> "WITHDRAW".equals(response.getOperationType())
                        && "PENDING".equals(response.getStatus()))
                .assertComplete();

        ArgumentCaptor<WalletOperationEvent> eventCaptor =
                ArgumentCaptor.forClass(WalletOperationEvent.class);
        verify(eventPublisher).publishWalletOperation(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getOperationType()).isEqualTo("WITHDRAW");
    }

    @Test
    @DisplayName("withdraw: falla si el monedero no tiene saldo suficiente")
    void withdraw_whenInsufficientBalance_shouldError() {
        sampleWallet.setDebitCardId("card-01");
        sampleWallet.setBalance(10.0);
        when(walletRepository.findById("w-1")).thenReturn(Mono.just(sampleWallet));

        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(50.0);

        walletService.withdraw("w-1", request).test()
                .assertError(IllegalArgumentException.class);
        verify(eventPublisher, never()).publishWalletOperation(any(WalletOperationEvent.class));
    }

    @Test
    @DisplayName("withdraw: falla si el importe no es positivo")
    void withdraw_whenInvalidAmount_shouldError() {
        WalletAmountRequest request = new WalletAmountRequest();
        request.setAmount(0.0);

        walletService.withdraw("w-1", request).test()
                .assertError(IllegalArgumentException.class);
    }
}
