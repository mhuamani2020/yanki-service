package com.nttdata.yanki_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Entidad de dominio que representa un monedero móvil <b>Yanki</b> (Parte III).
 * <p>
 * Se persiste en la colección {@code wallets} de MongoDB (patrón database-per-service). No
 * requiere que el titular sea cliente del banco: se identifica por su número de documento
 * (DNI, CEX o Pasaporte), su número de celular (con el que envía y recibe pagos), el IMEI de
 * su celular y su correo electrónico. Opcionalmente puede asociarse a una tarjeta de débito
 * del banco para cargar o acreditar saldo contra la cuenta principal de dicha tarjeta.
 *
 * @author NTT Data
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Document(collection = "wallets")
public class Wallet {

    /** Identificador único del monedero generado por MongoDB. */
    @Id
    private String id;

    /** Número de documento de identificación del usuario (DNI, CEX o Pasaporte). */
    private String documentNumber;

    /** Número de celular del usuario, usado para enviar y recibir pagos. */
    private String cellphone;

    /** IMEI del celular del usuario. */
    private String imei;

    /** Correo electrónico del usuario. */
    private String email;

    /** Saldo disponible del monedero. */
    private Double balance;

    /** Tarjeta de débito del banco asociada al monedero (opcional). */
    private String debitCardId;
}
