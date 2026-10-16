package com.nttdata.yanki_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Clase principal del microservicio del monedero móvil <b>Yanki</b> (Parte III).
 * <p>
 * Es un microservicio <b>nuevo</b>: se construye a partir de su contrato OpenAPI
 * (contract-first), implementa sus nuevas funcionalidades con <b>RxJava 3</b> y se comunica
 * con {@code bank-account-service} <b>exclusivamente por Kafka</b>, nunca por REST, conforme a
 * la restricción de la Parte III para microservicios nuevos.
 * <p>
 * Siguiendo el patrón database-per-service, gestiona sus propias colecciones
 * ({@code wallets} y {@code wallet_operations}) en MongoDB y no accede a datos de otros
 * microservicios.
 *
 * @author NTT Data
 */
@SpringBootApplication
public class YankiServiceApplication {

    /**
     * Punto de entrada de la aplicación Spring Boot.
     *
     * @param args argumentos de línea de comandos proporcionados al arrancar
     */
    public static void main(String[] args) {
        SpringApplication.run(YankiServiceApplication.class, args);
    }

}
