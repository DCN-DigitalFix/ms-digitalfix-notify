package cl.duoc.ms_digitalfix_notify.messaging;

import java.time.Instant;
import java.util.Map;

/**
 * Sobre comun a todos los mensajes del sistema.
 *
 * El productor (ms-digitalfix-bff, ms-digitalfix-workorders) publica siempre
 * esta forma, de modo que los consumidores pueden trazar, deduplicar y
 * registrar sin conocer el caso de uso concreto.
 *
 * @param type          que ocurrio, p. ej. "orden.asignada"
 * @param eventId       identificador unico del evento; es la clave de idempotencia
 * @param timestamp     momento en que el productor lo emitio
 * @param traceId       hilo de la peticion HTTP original, para seguirla entre servicios
 * @param correlationId agrupa mensajes de una misma orden de trabajo
 * @param datos         carga util propia del caso de uso
 */
public record Envelope(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        Map<String, Object> datos) {

    /** Lectura segura de un campo de texto de la carga util. */
    public String dato(String clave) {
        Object valor = datos == null ? null : datos.get(clave);
        return valor == null ? null : valor.toString();
    }

    /**
     * Un mensaje sin eventId no se puede deduplicar ni trazar, y sin type no
     * se sabe que hacer con el. Ambos casos son errores del productor: no
     * tiene sentido reintentarlos.
     */
    public boolean esValido() {
        return eventId != null && !eventId.isBlank()
                && type != null && !type.isBlank();
    }
}
