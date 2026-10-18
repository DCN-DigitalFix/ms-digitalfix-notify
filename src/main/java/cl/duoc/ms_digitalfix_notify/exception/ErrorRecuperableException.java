package cl.duoc.ms_digitalfix_notify.exception;

/**
 * Fallo transitorio: el mensaje era valido pero el intento no prospero.
 *
 * Tipicamente el proveedor externo no respondio (SMTP caido, timeout de la
 * pasarela de push, disco lleno al generar el PDF). Reintentar tiene sentido
 * porque la causa puede desaparecer sola.
 */
public class ErrorRecuperableException extends RuntimeException {

    public ErrorRecuperableException(String message) {
        super(message);
    }

    public ErrorRecuperableException(String message, Throwable cause) {
        super(message, cause);
    }
}
