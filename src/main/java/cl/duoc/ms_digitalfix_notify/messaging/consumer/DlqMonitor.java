package cl.duoc.ms_digitalfix_notify.messaging.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

/**
 * Registro de mensajes muertos.
 *
 * Las tres DLQ se escuchan aqui en lugar de repartirlas entre los
 * consumidores de dominio, porque lo que se hace con un mensaje muerto no
 * depende del caso de uso: se deja constancia y se retiene para analisis.
 *
 * Cada entrada del log incluye la razon y la cola de origen, que RabbitMQ
 * adjunta en la cabecera x-death al derivar el mensaje. Con eso se puede
 * reconstruir por que fallo sin abrir la Management UI.
 *
 * El contador por cola expone la tasa de DLQ, que es la senal mas util para
 * detectar que un flujo se rompio: si sube de golpe, algo aguas arriba esta
 * emitiendo mensajes invalidos o el proveedor externo esta caido.
 */
@Component
public class DlqMonitor {

    private static final Logger log = LoggerFactory.getLogger(DlqMonitor.class);

    private final Map<String, AtomicLong> conteo = new ConcurrentHashMap<>();

    @RabbitListener(queues = {
            "${digitalfix.rabbit.flujos.email.dlq}",
            "${digitalfix.rabbit.flujos.dispatch.dlq}",
            "${digitalfix.rabbit.flujos.report.dlq}"
    })
    public void registrar(
            Message mensaje,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        String cola = mensaje.getMessageProperties().getConsumerQueue();
        long total = conteo.computeIfAbsent(cola, k -> new AtomicLong()).incrementAndGet();

        log.error("[dlq] {} acumula {} mensajes muertos. Origen: {} · Razon: {} · Cuerpo: {}",
                cola,
                total,
                colaOriginal(mensaje),
                razon(mensaje),
                new String(mensaje.getBody()));

        // Se confirma para que la DLQ no crezca sin control. El mensaje ya
        // quedo en el log, que es el registro que pide la operacion.
        channel.basicAck(deliveryTag, false);
    }

    /** Total de mensajes muertos por cola desde que arranco el servicio. */
    public Map<String, Long> metricas() {
        return conteo.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get()));
    }

    /**
     * RabbitMQ agrega la cabecera x-death con el historial de derivaciones.
     * La primera entrada es la mas reciente.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> primerXDeath(Message mensaje) {
        Object header = mensaje.getMessageProperties().getHeaders().get("x-death");
        if (header instanceof List<?> lista && !lista.isEmpty()
                && lista.get(0) instanceof Map<?, ?> entrada) {
            return (Map<String, Object>) entrada;
        }
        return Map.of();
    }

    private String razon(Message mensaje) {
        Object reason = primerXDeath(mensaje).get("reason");
        return reason == null ? "desconocida" : reason.toString();
    }

    private String colaOriginal(Message mensaje) {
        Object queue = primerXDeath(mensaje).get("queue");
        return queue == null ? "desconocida" : queue.toString();
    }
}
