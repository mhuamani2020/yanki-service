package com.nttdata.yanki_service.repository;

import com.nttdata.yanki_service.model.WalletOperation;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Repositorio reactivo de las operaciones asíncronas de monedero ({@link WalletOperation})
 * sobre MongoDB, en la colección {@code wallet_operations} de este microservicio.
 *
 * @author NTT Data
 */
@Repository
public interface WalletOperationRepository extends ReactiveMongoRepository<WalletOperation, String> {
}
