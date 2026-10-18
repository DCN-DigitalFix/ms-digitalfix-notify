package cl.duoc.ms_digitalfix_notify.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.ms_digitalfix_notify.exception.ErrorNoRecuperableException;
import cl.duoc.ms_digitalfix_notify.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_notify.messaging.Envelope;

/**
 * Envio de notificaciones al cliente.
 *
 * No sabe nada de RabbitMQ: no importa colas, ni canales, ni ACK. Recibe un
 * Envelope ya deserializado y devuelve, o lanza una de las dos excepciones de
 * dominio. Quien decide reintentar o descartar es el consumidor.
 *
 * Esa separacion permite probar el envio sin levantar un broker, y permite
 * cambiar la topologia de mensajeria sin tocar esta clase.
 */
@Service
public class EmailService {

    private static final Logger log = LoggerFactory.getLogger(EmailService.class);

    public void enviar(Envelope envelope) {
        String destinatario = envelope.dato("destinatario");
        String ordenId = envelope.dato("ordenId");

        // Sin destinatario no hay nada que hacer, ni ahora ni en diez minutos.
        if (destinatario == null || destinatario.isBlank()) {
            throw new ErrorNoRecuperableException(
                    "El mensaje no trae destinatario (orden " + ordenId + ")");
        }

        if (!destinatario.contains("@")) {
            throw new ErrorNoRecuperableException(
                    "Direccion de correo mal formada: " + destinatario);
        }

        simularProveedor(envelope);

        log.info("Notificacion '{}' enviada a {} por la orden {}",
                envelope.type(), destinatario, ordenId);
    }

    /**
     * Punto de integracion con el proveedor de correo o push.
     *
     * Mientras no exista, el campo opcional "simularFallo" permite provocar
     * cada rama a voluntad, que es lo que hace demostrable el comportamiento
     * de reintentos y DLQ sin esperar a que se caiga un servicio real.
     */
    private void simularProveedor(Envelope envelope) {
        String fallo = envelope.dato("simularFallo");
        if (fallo == null) {
            return;
        }
        if (fallo.equals("recuperable")) {
            throw new ErrorRecuperableException("El proveedor de correo no respondio");
        }
        if (fallo.equals("permanente")) {
            throw new ErrorNoRecuperableException("El proveedor rechazo la direccion de forma definitiva");
        }
    }
}
