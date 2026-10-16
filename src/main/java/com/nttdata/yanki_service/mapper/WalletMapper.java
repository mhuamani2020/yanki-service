package com.nttdata.yanki_service.mapper;

import com.nttdata.yanki_service.dto.WalletOperationResponse;
import com.nttdata.yanki_service.dto.WalletResponse;
import com.nttdata.yanki_service.model.Wallet;
import com.nttdata.yanki_service.model.WalletOperation;

/**
 * Utilidades de mapeo entre las entidades de dominio ({@link Wallet} y
 * {@link WalletOperation}) y los DTOs del contrato API.
 *
 * @author NTT Data
 */
public class WalletMapper {

    /** Clase de utilidad: no se instancia. */
    private WalletMapper() {
    }

    /**
     * Convierte la entidad {@link Wallet} en su DTO de respuesta.
     *
     * @param wallet monedero a transformar
     * @return DTO de respuesta del monedero
     */
    public static WalletResponse toResponse(Wallet wallet) {
        WalletResponse response = new WalletResponse();
        response.setId(wallet.getId());
        response.setDocumentNumber(wallet.getDocumentNumber());
        response.setCellphone(wallet.getCellphone());
        response.setImei(wallet.getImei());
        response.setEmail(wallet.getEmail());
        response.setBalance(wallet.getBalance());
        response.setDebitCardId(wallet.getDebitCardId());
        return response;
    }

    /**
     * Convierte la entidad {@link WalletOperation} en su DTO de respuesta.
     *
     * @param operation operación a transformar
     * @return DTO de respuesta de la operación
     */
    public static WalletOperationResponse toResponse(WalletOperation operation) {
        WalletOperationResponse response = new WalletOperationResponse();
        response.setOperationId(operation.getId());
        response.setWalletId(operation.getWalletId());
        response.setOperationType(operation.getOperationType());
        response.setAmount(operation.getAmount());
        response.setStatus(operation.getStatus());
        return response;
    }
}
