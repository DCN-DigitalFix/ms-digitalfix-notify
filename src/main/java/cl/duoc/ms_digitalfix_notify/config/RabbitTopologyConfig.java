package cl.duoc.ms_digitalfix_notify.config;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declara la topologia de RabbitMQ como beans de Spring.
 *
 * Al arrancar, RabbitAdmin recorre estos beans y crea en el broker lo que
 * falte: tres exchanges, tres colas principales y tres DLQ, con sus bindings.
 * Es idempotente, asi que levantar el servicio contra un broker ya configurado
 * no rompe nada.
 *
 * Cada caso de uso del dominio (email, dispatch, report) tiene su propio
 * bloque de beans, con el mismo patron:
 *
 *   cmd.direct  --routingKeyDirect-->  cola principal
 *   cmd.topic   --routingKeyTopic -->  cola principal
 *   cola principal  --(al fallar)-->  cmd.dead.dlx  --routingKeyDlq-->  DLQ
 *
 * La derivacion a la DLQ no se programa: se declara con los argumentos
 * x-dead-letter-exchange y x-dead-letter-routing-key de la cola principal.
 * Cuando un consumidor hace NACK sin requeue, el broker mueve el mensaje solo.
 *
 * Esta clase no contiene logica de negocio y ninguna clase de negocio contiene
 * configuracion de mensajeria.
 */
@Configuration
@EnableConfigurationProperties(RabbitProperties.class)
public class RabbitTopologyConfig {

    @Autowired
    private RabbitProperties props;

    // -----------------------------------------------------------------------
    // Exchanges
    // -----------------------------------------------------------------------

    /** Comandos con destinatario unico y conocido. */
    @Bean
    public DirectExchange cmdDirectExchange() {
        return new DirectExchange(props.exchanges().direct(), true, false);
    }

    /** Comandos enrutados por patron: email.*, dispatch.#, report.* */
    @Bean
    public TopicExchange cmdTopicExchange() {
        return new TopicExchange(props.exchanges().topic(), true, false);
    }

    /**
     * Dead letter exchange. Es direct porque cada cola principal apunta a su
     * DLQ con una clave exacta, sin ambiguedad.
     */
    @Bean
    public DirectExchange cmdDeadLetterExchange() {
        return new DirectExchange(props.exchanges().dlx(), true, false);
    }

    // -----------------------------------------------------------------------
    // Caso de uso: notificacion por email / push al cliente
    // -----------------------------------------------------------------------

    @Bean
    public Queue colaEmail() {
        return colaPrincipal(props.flujos().email());
    }

    @Bean
    public Queue dlqEmail() {
        return new Queue(props.flujos().email().dlq(), true);
    }

    @Bean
    public Binding bindingEmailDirect() {
        return BindingBuilder.bind(colaEmail())
                .to(cmdDirectExchange())
                .with(props.flujos().email().routingKeyDirect());
    }

    @Bean
    public Binding bindingEmailTopic() {
        return BindingBuilder.bind(colaEmail())
                .to(cmdTopicExchange())
                .with(props.flujos().email().routingKeyTopic());
    }

    @Bean
    public Binding bindingEmailDlq() {
        return BindingBuilder.bind(dlqEmail())
                .to(cmdDeadLetterExchange())
                .with(props.flujos().email().routingKeyDlq());
    }

    // -----------------------------------------------------------------------
    // Caso de uso: ticket de despacho a terreno
    // -----------------------------------------------------------------------

    @Bean
    public Queue colaDispatch() {
        return colaPrincipal(props.flujos().dispatch());
    }

    @Bean
    public Queue dlqDispatch() {
        return new Queue(props.flujos().dispatch().dlq(), true);
    }

    @Bean
    public Binding bindingDispatchDirect() {
        return BindingBuilder.bind(colaDispatch())
                .to(cmdDirectExchange())
                .with(props.flujos().dispatch().routingKeyDirect());
    }

    @Bean
    public Binding bindingDispatchTopic() {
        return BindingBuilder.bind(colaDispatch())
                .to(cmdTopicExchange())
                .with(props.flujos().dispatch().routingKeyTopic());
    }

    @Bean
    public Binding bindingDispatchDlq() {
        return BindingBuilder.bind(dlqDispatch())
                .to(cmdDeadLetterExchange())
                .with(props.flujos().dispatch().routingKeyDlq());
    }

    // -----------------------------------------------------------------------
    // Caso de uso: generacion de informe tecnico en PDF
    // -----------------------------------------------------------------------

    @Bean
    public Queue colaReport() {
        return colaPrincipal(props.flujos().report());
    }

    @Bean
    public Queue dlqReport() {
        return new Queue(props.flujos().report().dlq(), true);
    }

    @Bean
    public Binding bindingReportDirect() {
        return BindingBuilder.bind(colaReport())
                .to(cmdDirectExchange())
                .with(props.flujos().report().routingKeyDirect());
    }

    @Bean
    public Binding bindingReportTopic() {
        return BindingBuilder.bind(colaReport())
                .to(cmdTopicExchange())
                .with(props.flujos().report().routingKeyTopic());
    }

    @Bean
    public Binding bindingReportDlq() {
        return BindingBuilder.bind(dlqReport())
                .to(cmdDeadLetterExchange())
                .with(props.flujos().report().routingKeyDlq());
    }

    // -----------------------------------------------------------------------

    /**
     * Cola durable apuntando al DLX. Los tres flujos comparten esta forma, asi
     * que se construye una sola vez y cada bean le pasa su propio flujo.
     */
    private Queue colaPrincipal(RabbitProperties.Flujo flujo) {
        return QueueBuilder.durable(flujo.cola())
                .withArguments(Map.of(
                        "x-dead-letter-exchange", props.exchanges().dlx(),
                        "x-dead-letter-routing-key", flujo.routingKeyDlq()))
                .build();
    }
}
