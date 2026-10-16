package com.nttdata.yanki_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nttdata.yanki_service.config.KafkaTopicsProperties;
import com.nttdata.yanki_service.event.WalletOperationEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pruebas unitarias de {@link KafkaEventPublisher} con el {@code KafkaTemplate} mockeado
 * (Parte III: los nuevos métodos públicos deben tener sus pruebas con mocks).
 *
 * @author NTT Data
 */
@ExtendWith(MockitoExtension.class)
class KafkaEventPublisherTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaEventPublisher publisher;

    @BeforeEach
    void setUp() {
        KafkaTopicsProperties topics = new KafkaTopicsProperties();
        topics.setWalletOperations("wallet-operations");
        publisher = new KafkaEventPublisher(kafkaTemplate, new ObjectMapper(), topics);
    }

    @Test
    @DisplayName("publishWalletOperation envía el comando al topic wallet-operations")
    void publishWalletOperation_shouldSendToTopic() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        WalletOperationEvent event = WalletOperationEvent.builder()
                .operationId("op-1").operationType("TOP_UP").debitCardId("card-01")
                .amount(100.0).requestedAt("2026-01-01T00:00:00Z").build();

        publisher.publishWalletOperation(event).test().assertComplete();

        verify(kafkaTemplate).send(eq("wallet-operations"), eq("op-1"), anyString());
    }

    @Test
    @DisplayName("Un fallo al publicar no propaga error (publicación best-effort)")
    void publish_whenKafkaFails_shouldCompleteWithoutError() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("broker no disponible"));

        WalletOperationEvent event = WalletOperationEvent.builder().operationId("op-1").build();

        publisher.publishWalletOperation(event).test().assertComplete();
    }
}
