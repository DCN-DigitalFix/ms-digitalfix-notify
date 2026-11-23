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
import cl.duoc.ms_digitalfix_notify.service.ReportService;

/**
 * Dominio: generacion de documentos.
 *
 * Consume q.cmd.report, alimentada por la clave report.gen en cmd.direct y
 * por el patron report.* en cmd.topic.
 *
 * Generar un PDF es costoso, asi que el limite de reintentos es bajo: si
 * falla un par de veces conviene revisarlo a mano antes que seguir gastando
 * CPU en la misma operacion.
 */
@Component
public class ReportConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReportConsumer.class);

    @Autowired
    private ReportService reportService;

    @Autowired
    private ControlReintentos control;

    @Value("${digitalfix.rabbit.flujos.report.max-reintentos}")
    private int maxReintentos;

    @RabbitListener(queues = "${digitalfix.rabbit.flujos.report.cola}")
    public void recibir(
            Envelope envelope,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        if (envelope == null || !envelope.esValido()) {
            log.error("[report] mensaje sin eventId o sin type. Se deriva a la DLQ");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        if (control.yaFueProcesado(envelope.eventId())) {
            log.info("[report] evento {} ya procesado, se descarta el duplicado", envelope.eventId());
            channel.basicAck(deliveryTag, false);
            return;
        }

        int intento = control.registrarIntento(envelope.eventId());

        try {
            reportService.generar(envelope);

            control.marcarProcesado(envelope.eventId());
            channel.basicAck(deliveryTag, false);
            log.info("[report] evento {} procesado en el intento {}", envelope.eventId(), intento);

        } catch (ErrorNoRecuperableException e) {
            log.error("[report] evento {} no recuperable: {}. Se deriva a la DLQ",
                    envelope.eventId(), e.getMessage());
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);

        } catch (ErrorRecuperableException e) {
            if (intento < maxReintentos) {
                log.warn("[report] evento {} fallo en el intento {} de {}: {}. Se reencola",
                        envelope.eventId(), intento, maxReintentos, e.getMessage());
                channel.basicNack(deliveryTag, false, true);
            } else {
                log.error("[report] evento {} agoto sus {} intentos: {}. Se deriva a la DLQ",
                        envelope.eventId(), maxReintentos, e.getMessage());
                control.descartar(envelope.eventId());
                channel.basicNack(deliveryTag, false, false);
            }

        } catch (Exception e) {
            log.error("[report] evento {} fallo con un error inesperado. Se deriva a la DLQ",
                    envelope.eventId(), e);
            control.descartar(envelope.eventId());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
