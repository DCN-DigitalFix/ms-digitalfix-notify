package cl.duoc.ms_digitalfix_notify.messaging.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.ms_digitalfix_notify.exception.ErrorNoRecuperableException;
import cl.duoc.ms_digitalfix_notify.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_notify.messaging.ControlReintentos;
import cl.duoc.ms_digitalfix_notify.messaging.Envelope;
import cl.duoc.ms_digitalfix_notify.service.EmailService;

/**
 * Dominio: notificacion al cliente.
 *
 * Consume q.cmd.email, que recibe mensajes por dos caminos: la clave exacta
 * email.send en cmd.direct y el patron email.* en cmd.topic.
 *
 * El ACK es manual. Eso obliga a decidir explicitamente que hacer con cada
 * mensaje y es lo que evita dos problemas opuestos: confirmar un mensaje que
 * fallo (se pierde) o reintentar para siempre uno que nunca va a funcionar
 * (bloquea la cola).
 *
 * Las cuatro salidas posibles son:
 *
 *   exito              -> basicAck
 *   duplicado          -> basicAck, sin reprocesar
 *   error permanente   -> basicNack(requeue=false) -> DLQ
 *   error transitorio  -> basicNack(requeue=true) mientras queden intentos,
 *                         y basicNack(requeue=false) -> DLQ al agotarlos
 *
 * La derivacion a q.cmd.email.dlq no se programa aqui: la cola se declaro con
 * x-dead-letter-exchange, asi que el broker la hace al recibir el NACK.
 */
@Component
public class EmailConsumer {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);

    private final EmailService emailService;
    private final ControlReintentos control;
    private final int maxReintentos;

    public EmailConsumer(
            EmailService emailService,
            ControlReintentos control,
            @Value("${digitalfix.rabbit.flujos.email.max-reintentos}") int maxReintentos) {
        this.emailService = emailService;
        this.control = control;
        this.maxReintentos = maxReintentos;
    }

    @RabbitListener(queues = "${digitalfix.rabbit.flujos.email.cola}")
    public void recibir(
            Envelope envelope,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        // Mensaje mal formado: le falta eventId o type. Es un error del
        // productor, reintentarlo daria exactamente el mismo resultado.
        if (envelope == null || !envelope.esValido()) {
            log.error("[email] mensaje sin eventId o sin type. Se deriva a la DLQ");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // RabbitMQ entrega al menos una vez, asi que un evento puede repetirse
        // si el ACK anterior se perdio. Se confirma sin volver a enviar nada.
        if (control.yaFueProcesado(envelope.eventId())) {
            log.info("[email] evento {} ya procesado, se descarta el duplicado", envelope.eventId());
            channel.basicAck(deliveryTag, false);
            return;
        }

        int intento = control.registrarIntento(envelope.eventId());

        try {
            emailService.enviar(envelope);

            control.marcarProcesado(envelope.eventId());
            channel.basicAck(deliveryTag, false);
            log.info("[email] evento {} procesado en el intento {}", envelope.eventId(), intento);

        } catch (ErrorNoRecuperableException e) {
            log.error("[email] evento {} no recuperable: {}. Se deriva a la DLQ",
                    envelope.eventId(), e.getMessage());
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);

        } catch (ErrorRecuperableException e) {
            if (intento < maxReintentos) {
                log.warn("[email] evento {} fallo en el intento {} de {}: {}. Se reencola",
                        envelope.eventId(), intento, maxReintentos, e.getMessage());
                // requeue=true devuelve el mensaje a la cola. El reintento es
                // inmediato: en produccion convendria una cola de espera con
                // TTL para espaciarlos, pero aqui basta con el limite de
                // intentos para que no gire indefinidamente.
                channel.basicNack(deliveryTag, false, true);
            } else {
                log.error("[email] evento {} agoto sus {} intentos: {}. Se deriva a la DLQ",
                        envelope.eventId(), maxReintentos, e.getMessage());
                control.descartar(envelope.eventId());
                channel.basicNack(deliveryTag, false, false);
            }

        } catch (Exception e) {
            // Un fallo que el servicio no clasifico. Se trata como permanente
            // a proposito: reencolar algo que no se entiende arriesga un ciclo
            // infinito, y en la DLQ queda disponible para revisarlo.
            log.error("[email] evento {} fallo con un error inesperado. Se deriva a la DLQ",
                    envelope.eventId(), e);
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
