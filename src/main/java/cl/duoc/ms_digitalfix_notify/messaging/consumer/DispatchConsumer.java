package cl.duoc.ms_digitalfix_notify.messaging.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.ms_digitalfix_notify.exception.ErrorNoRecuperableException;
import cl.duoc.ms_digitalfix_notify.exception.ErrorRecuperableException;
import cl.duoc.ms_digitalfix_notify.messaging.ControlReintentos;
import cl.duoc.ms_digitalfix_notify.messaging.Envelope;
import cl.duoc.ms_digitalfix_notify.service.DispatchService;

/**
 * Dominio: despacho de cuadrillas a terreno.
 *
 * Consume q.cmd.dispatch, alimentada por la clave dispatch.ticket en
 * cmd.direct y por el patron dispatch.# en cmd.topic.
 *
 * Este flujo tolera mas reintentos que los otros dos: si el ticket no llega,
 * el tecnico no sale, y la orden se queda detenida. Por eso el limite
 * configurado es mayor.
 */
@Component
public class DispatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(DispatchConsumer.class);

    private final DispatchService dispatchService;
    private final ControlReintentos control;
    private final int maxReintentos;

    @Autowired
    public DispatchConsumer(
            DispatchService dispatchService,
            ControlReintentos control,
            @Value("${digitalfix.rabbit.flujos.dispatch.max-reintentos}") int maxReintentos) {
        this.dispatchService = dispatchService;
        this.control = control;
        this.maxReintentos = maxReintentos;
    }

    @RabbitListener(queues = "${digitalfix.rabbit.flujos.dispatch.cola}")
    public void recibir(
            Envelope envelope,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        if (envelope == null || !envelope.esValido()) {
            log.error("[dispatch] mensaje sin eventId o sin type. Se deriva a la DLQ");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // Emitir dos veces el mismo ticket mandaria dos cuadrillas al mismo
        // domicilio, asi que la deduplicacion importa mas que en otros flujos.
        if (control.yaFueProcesado(envelope.eventId())) {
            log.info("[dispatch] evento {} ya procesado, se descarta el duplicado", envelope.eventId());
            channel.basicAck(deliveryTag, false);
            return;
        }

        int intento = control.registrarIntento(envelope.eventId());

        try {
            dispatchService.emitirTicket(envelope);

            control.marcarProcesado(envelope.eventId());
            channel.basicAck(deliveryTag, false);
            log.info("[dispatch] evento {} procesado en el intento {}", envelope.eventId(), intento);

        } catch (ErrorNoRecuperableException e) {
            log.error("[dispatch] evento {} no recuperable: {}. Se deriva a la DLQ",
                    envelope.eventId(), e.getMessage());
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);

        } catch (ErrorRecuperableException e) {
            if (intento < maxReintentos) {
                log.warn("[dispatch] evento {} fallo en el intento {} de {}: {}. Se reencola",
                        envelope.eventId(), intento, maxReintentos, e.getMessage());
                channel.basicNack(deliveryTag, false, true);
            } else {
                log.error("[dispatch] evento {} agoto sus {} intentos: {}. Se deriva a la DLQ",
                        envelope.eventId(), maxReintentos, e.getMessage());
                control.descartar(envelope.eventId());
                channel.basicNack(deliveryTag, false, false);
            }

        } catch (Exception e) {
            log.error("[dispatch] evento {} fallo con un error inesperado. Se deriva a la DLQ",
                    envelope.eventId(), e);
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
