package cl.duoc.ms_digitalfix_notify.service;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_notify.exception.ErrorNoRecuperableException;
import cl.duoc.ms_digitalfix_notify.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_notify.messaging.Envelope;

/**
 * Generacion de documentos en PDF: informe tecnico y certificado de trabajo.
 *
 * Sin dependencias de mensajeria, como los demas servicios de este paquete.
 */
@Service
public class ReportService {

    private static final Logger log = LoggerFactory.getLogger(ReportService.class);

    private static final Set<String> TIPOS_SOPORTADOS = Set.of("informe", "certificado");

    public void generar(Envelope envelope) {
        String ordenId = envelope.dato("ordenId");
        String documento = envelope.dato("documento");

        if (ordenId == null || ordenId.isBlank()) {
            throw new ErrorNoRecuperableException("La solicitud no indica la orden de trabajo");
        }

        // Un tipo desconocido no se arregla con el tiempo: o el productor
        // escribio mal el campo, o pide algo que este servicio no genera.
        if (documento == null || !TIPOS_SOPORTADOS.contains(documento)) {
            throw new ErrorNoRecuperableException(
                    "Tipo de documento no soportado: " + documento
                            + ". Validos: " + TIPOS_SOPORTADOS);
        }

        simularProveedor(envelope);

        log.info("Documento '{}' generado para la orden {}", documento, ordenId);
    }

    /** Ver la nota en EmailService: hook para demostrar reintentos y DLQ. */
    private void simularProveedor(Envelope envelope) {
        String fallo = envelope.dato("simularFallo");
        if (fallo == null) {
            return;
        }
        if (fallo.equals("recuperable")) {
            throw new ErrorRecuperableException("No hay espacio para escribir el PDF");
        }
        if (fallo.equals("permanente")) {
            throw new ErrorNoRecuperableException("La plantilla del documento no existe");
        }
    }
}
