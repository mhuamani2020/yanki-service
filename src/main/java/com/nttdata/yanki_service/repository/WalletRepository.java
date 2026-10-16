package com.nttdata.yanki_service.repository;

import com.nttdata.yanki_service.model.Wallet;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Repositorio reactivo de la entidad {@link Wallet} sobre MongoDB.
 * <p>
 * Proporciona las operaciones CRUD reactivas (Flux/Mono) únicamente sobre la colección
 * {@code wallets} de este microservicio, conforme al patrón database-per-service.
 *
 * @author NTT Data
 */
@Repository
public interface WalletRepository extends ReactiveMongoRepository<Wallet, String> {

    /**
     * Busca un monedero por su número de celular.
     *
     * @param cellphone número de celular del monedero
     * @return mono con el monedero encontrado; vacío si no existe
     */
    Mono<Wallet> findByCellphone(String cellphone);

    /**
     * Comprueba si ya existe un monedero con el número de celular indicado.
     *
     * @param cellphone número de celular a comprobar
     * @return mono con {@code true} si existe
     */
    Mono<Boolean> existsByCellphone(String cellphone);
}
