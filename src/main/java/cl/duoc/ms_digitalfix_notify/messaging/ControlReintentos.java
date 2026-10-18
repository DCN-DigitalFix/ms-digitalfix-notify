package cl.duoc.ms_digitalfix_notify.messaging;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

/**
 * Lleva la cuenta de intentos por evento y recuerda cuales ya se procesaron.
 *
 * Son dos responsabilidades cercanas sobre el mismo identificador:
 *
 *   - Reintentos: cuantas veces fallo un evento recuperable. El consumidor lo
 *     consulta para decidir si vuelve a encolar o lo manda a la DLQ.
 *   - Idempotencia: RabbitMQ garantiza entrega al menos una vez, asi que un
 *     mismo evento puede llegar dos veces (por ejemplo si el ACK se pierde al
 *     caer un nodo del cluster). Sin este control, el cliente recibiria el
 *     correo duplicado.
 *
 * El almacenamiento es en memoria, lo que basta para una sola instancia. Con
 * varias replicas habria que moverlo a Redis o a una tabla, porque cada
 * instancia tendria su propia copia.
 */
@Component
public class ControlReintentos {

    private final Map<String, AtomicInteger> intentos = new ConcurrentHashMap<>();
    private final Set<String> procesados = ConcurrentHashMap.newKeySet();

    /** Suma un intento y devuelve el numero del intento actual, partiendo en 1. */
    public int registrarIntento(String eventId) {
        return intentos.computeIfAbsent(eventId, k -> new AtomicInteger(0)).incrementAndGet();
    }

    public boolean yaFueProcesado(String eventId) {
        return procesados.contains(eventId);
    }

    /** Exito: se recuerda el evento y se libera el contador. */
    public void marcarProcesado(String eventId) {
        procesados.add(eventId);
        intentos.remove(eventId);
    }

    /** Derivado a la DLQ: ya no se reintenta, el contador deja de ser util. */
    public void descartar(String eventId) {
        intentos.remove(eventId);
    }
}
