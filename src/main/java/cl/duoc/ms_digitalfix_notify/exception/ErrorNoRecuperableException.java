package cl.duoc.ms_digitalfix_notify.exception;

/**
 * Fallo permanente: el mensaje nunca va a poder procesarse tal como viene.
 *
 * Tipicamente le falta un campo obligatorio, trae una direccion mal formada o
 * un tipo de evento desconocido. Reintentar solo ocuparia la cola, asi que va
 * directo a la DLQ para revision manual.
 */
public class ErrorNoRecuperableException extends RuntimeException {

    public ErrorNoRecuperableException(String message) {
        super(message);
    }

    public ErrorNoRecuperableException(String message, Throwable cause) {
        super(message, cause);
    }
}
