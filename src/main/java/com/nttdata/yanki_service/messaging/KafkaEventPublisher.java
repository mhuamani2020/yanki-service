package com.nttdata.yanki_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nttdata.yanki_service.config.KafkaTopicsProperties;
import com.nttdata.yanki_service.event.WalletOperationEvent;
import io.reactivex.rxjava3.core.Completable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publicador de comandos de operación de monedero hacia Kafka (Parte III).
 * <p>
 * Traduce el comando a JSON y lo envía al topic {@code wallet-operations}, que consume
 * {@code bank-account-service}. Es el único canal de comunicación con ese microservicio (no se
 * usan peticiones REST). La publicación es <b>best-effort</b>: cualquier fallo se registra en
 * el log y no interrumpe la operación que originó el comando.
 * <p>
 * La API pública devuelve tipos de <b>RxJava 3</b> ({@link Completable}) para mantener la
 * reactividad de las nuevas funcionalidades.
 *
 * @author NTT Data
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaTopicsProperties topics;

    /**
     * Publica el comando de una operación de monedero.
     *
     * @param event comando a enviar
     * @return {@code Completable} que notifica cuándo terminó el intento de publicación
     */
    public Completable publishWalletOperation(WalletOperationEvent event) {
        return Completable.fromAction(() -> {
                    String json = objectMapper.writeValueAsString(event);
                    kafkaTemplate.send(topics.getWalletOperations(), event.getOperationId(), json)
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    log.error("Error publishing wallet operation to topic {}: {}",
                                            topics.getWalletOperations(), ex.getMessage());
                                } else {
                                    log.debug("Published wallet operation to topic {}",
                                            topics.getWalletOperations());
                                }
                            });
                })
                .onErrorComplete(ex -> {
                    log.error("Could not serialize/publish wallet operation: {}", ex.getMessage());
                    return true;
                });
    }
}
