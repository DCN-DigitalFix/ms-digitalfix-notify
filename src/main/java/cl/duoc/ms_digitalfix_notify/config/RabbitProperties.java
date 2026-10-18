package cl.duoc.ms_digitalfix_notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topologia de mensajeria, leida desde application.yaml bajo "digitalfix.rabbit".
 *
 * Es la unica fuente de nombres de colas, exchanges y routing keys del
 * microservicio. Las clases de configuracion y los consumidores reciben estos
 * valores inyectados; ninguna escribe un nombre literal.
 */
@ConfigurationProperties(prefix = "digitalfix.rabbit")
public record RabbitProperties(Exchanges exchanges, Flujos flujos) {

    /**
     * @param direct exchange de comandos dirigidos a un unico consumidor
     * @param topic  exchange de comandos con enrutamiento por patron
     * @param dlx    dead letter exchange al que se derivan los mensajes fallidos
     */
    public record Exchanges(String direct, String topic, String dlx) {}

    /** Un flujo por caso de uso del dominio. */
    public record Flujos(Flujo email, Flujo dispatch, Flujo report) {}

    /**
     * Definicion completa de un caso de uso de mensajeria.
     *
     * @param cola             cola principal que consume el servicio
     * @param dlq              cola de mensajes muertos asociada
     * @param routingKeyDirect clave exacta en cmd.direct
     * @param routingKeyTopic  patron en cmd.topic
     * @param routingKeyDlq    clave con la que el mensaje llega a la DLQ
     * @param maxReintentos    intentos antes de derivar a la DLQ
     */
    public record Flujo(
            String cola,
            String dlq,
            String routingKeyDirect,
            String routingKeyTopic,
            String routingKeyDlq,
            int maxReintentos) {}
}
