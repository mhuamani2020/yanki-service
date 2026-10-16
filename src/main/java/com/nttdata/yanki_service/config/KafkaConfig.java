package com.nttdata.yanki_service.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Configuración de Kafka del productor de comandos de monedero (Parte III).
 * <p>
 * Se declara un {@link KafkaTemplate} tipado a {@code <String, String>} porque los eventos
 * viajan como JSON serializado en texto; de esta forma el contrato con
 * {@code bank-account-service} no depende de las clases Java de cada repositorio.
 * <p>
 * La configuración de conexión ({@code spring.kafka.*}) se mantiene externalizada en los
 * ficheros {@code application.yml} y {@code bank-config-repo}, nunca en el código.
 *
 * @author NTT Data
 */
@Configuration
public class KafkaConfig {

    /**
     * Fábrica de productores que envía claves y valores como {@code String}.
     *
     * @param kafkaProperties propiedades de Kafka externalizadas
     * @param sslBundles      bundles SSL gestionados por Spring Boot (vacío por defecto)
     * @return fábrica de productores de texto
     */
    @Bean
    public ProducerFactory<String, String> stringProducerFactory(KafkaProperties kafkaProperties,
                                                                 SslBundles sslBundles) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties(sslBundles));
    }

    /**
     * Plantilla de envío de comandos a Kafka.
     *
     * @param producerFactory fábrica de productores de texto
     * @return plantilla tipada para publicar eventos JSON
     */
    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
