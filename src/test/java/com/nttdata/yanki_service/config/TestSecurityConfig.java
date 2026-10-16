package com.nttdata.yanki_service.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Configuración de seguridad exclusiva para las pruebas de controlador
 * ({@code @WebFluxTest}).
 * <p>
 * Sustituye la cadena de filtros real (que exige JWT y requiere la clave externalizada) por
 * una que permite todas las peticiones, de modo que las pruebas se centren en el controlador y
 * no en la capa de seguridad.
 *
 * @author NTT Data
 */
@TestConfiguration
public class TestSecurityConfig {

    /**
     * Cadena de filtros que permite todas las peticiones en el entorno de pruebas.
     *
     * @param http configuración de seguridad reactiva
     * @return cadena de filtros permisiva
     */
    @Bean
    public SecurityWebFilterChain testSecurityWebFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchanges -> exchanges.anyExchange().permitAll())
                .build();
    }
}
