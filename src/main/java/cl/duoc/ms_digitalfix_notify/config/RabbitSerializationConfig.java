package cl.duoc.ms_digitalfix_notify.config;

import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Serializacion de mensajes.
 *
 * El cuerpo viaja como JSON para que cualquier productor pueda publicar sin
 * depender de las clases Java de este servicio.
 *
 * Hay dos ajustes que no son opcionales:
 *
 * JavaTimeModule, porque el envelope lleva un Instant y Jackson no lo maneja
 * por defecto.
 *
 * TypePrecedence.INFERRED, porque el productor escribe en la cabecera
 * __TypeId__ el nombre completo de SU clase Envelope, que vive en
 * cl.duoc.ms_digitalfix_bff.messaging. Esa clase no existe aqui, asi que con
 * el comportamiento por defecto la deserializacion falla con
 * ClassNotFoundException antes de que el consumidor vea el mensaje. INFERRED
 * le dice al convertidor que ignore la cabecera y use el tipo declarado en el
 * parametro del metodo @RabbitListener, que es lo correcto cuando el contrato
 * entre servicios es el JSON y no la clase.
 */
@Configuration
public class RabbitSerializationConfig {

    @Bean
    public MessageConverter jsonMessageConverter() {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(mapper);

        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
        typeMapper.setTrustedPackages(
                "cl.duoc.ms_digitalfix_notify.messaging",
                "cl.duoc.ms_digitalfix_workorders.messaging",
                "cl.duoc.ms_digitalfix_bff.messaging");
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }
}
