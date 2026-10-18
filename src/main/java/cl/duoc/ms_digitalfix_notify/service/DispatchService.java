package cl.duoc.ms_digitalfix_notify.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_notify.exception.ErrorNoRecuperableException;
import cl.duoc.ms_digitalfix_notify.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_notify.messaging.Envelope;

/**
 * Emision del ticket de despacho a terreno.
 *
 * Igual que EmailService, no conoce la infraestructura de mensajeria: solo
 * logica de negocio y las dos excepciones de dominio.
 */
@Service
public class DispatchService {

    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);

    public void emitirTicket(Envelope envelope) {
        String ordenId = envelope.dato("ordenId");
        String cuadrilla = envelope.dato("cuadrilla");
        String direccion = envelope.dato("direccion");

        if (ordenId == null || ordenId.isBlank()) {
            throw new ErrorNoRecuperableException("El ticket no indica a que orden pertenece");
        }

        // Un ticket sin cuadrilla no se puede repartir, y sin direccion el
        // tecnico no sabe adonde ir. Ambos son errores del productor.
        if (cuadrilla == null || cuadrilla.isBlank()) {
            throw new ErrorNoRecuperableException(
                    "La orden " + ordenId + " no tiene cuadrilla asignada");
        }

        if (direccion == null || direccion.isBlank()) {
            throw new ErrorNoRecuperableException(
                    "La orden " + ordenId + " no trae direccion de destino");
        }

        simularProveedor(envelope);

        log.info("Ticket de despacho emitido para la orden {} · cuadrilla {} · destino {}",
                ordenId, cuadrilla, direccion);
    }

    /** Ver la nota en EmailService: hook para demostrar reintentos y DLQ. */
    private void simularProveedor(Envelope envelope) {
        String fallo = envelope.dato("simularFallo");
        if (fallo == null) {
            return;
        }
        if (fallo.equals("recuperable")) {
            throw new ErrorRecuperableException("El servicio de despacho no esta disponible");
        }
        if (fallo.equals("permanente")) {
            throw new ErrorNoRecuperableException("La cuadrilla indicada no existe");
        }
    }
}
