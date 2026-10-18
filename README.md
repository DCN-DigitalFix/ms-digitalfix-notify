# ms-digitalfix-notify

Consumidor de RabbitMQ del sistema DigitalFix. Procesa de forma asíncrona las
tareas que no deben bloquear el flujo principal de una orden de trabajo:
notificar al cliente, emitir el ticket de despacho a terreno y generar
documentos en PDF.

No expone API de negocio. Su entrada son las colas.

---

## Topología

Tres flujos, cada uno con su cola principal y su DLQ:

| Cola | DLQ | `cmd.direct` | `cmd.topic` | Reintentos |
|---|---|---|---|---|
| `q.cmd.email` | `q.cmd.email.dlq` | `email.send` | `email.*` | 3 |
| `q.cmd.dispatch` | `q.cmd.dispatch.dlq` | `dispatch.ticket` | `dispatch.#` | 5 |
| `q.cmd.report` | `q.cmd.report.dlq` | `report.gen` | `report.*` | 2 |

Exchanges: `cmd.direct` (direct), `cmd.topic` (topic) y `cmd.dead.dlx`
(direct, para las DLQ).

Cada cola principal se declara con `x-dead-letter-exchange` apuntando a
`cmd.dead.dlx`, así que la derivación a la DLQ la hace el broker: el
consumidor solo tiene que hacer NACK sin reencolar.

Los nombres no están escritos en ninguna clase. Viven en `application.yaml`
bajo `digitalfix.rabbit` y se inyectan vía `RabbitProperties`.

---

## Cómo trata cada mensaje

El ACK es manual (`acknowledge-mode: manual`). Eso obliga a que cada
consumidor decida explícitamente, y evita los dos extremos: confirmar algo que
falló, o reintentar para siempre algo que nunca va a funcionar.

| Situación | Decisión |
|---|---|
| Procesado correctamente | `basicAck` |
| `eventId` ya visto | `basicAck`, sin reprocesar |
| Mensaje inválido (sin `eventId` o sin `type`) | `basicNack(requeue=false)` → DLQ |
| `ErrorNoRecuperableException` | `basicNack(requeue=false)` → DLQ |
| `ErrorRecuperableException`, quedan intentos | `basicNack(requeue=true)` |
| `ErrorRecuperableException`, intentos agotados | `basicNack(requeue=false)` → DLQ |
| Excepción no clasificada | `basicNack(requeue=false)` → DLQ |

La diferencia entre las dos excepciones es si el tiempo puede arreglar el
problema. Un SMTP caído sí (recuperable); un correo sin arroba, no
(permanente).

El reintento es inmediato, sin espera entre intentos. En producción
correspondería una cola intermedia con TTL; aquí el límite de intentos basta
para que no gire indefinidamente.

### Idempotencia

RabbitMQ garantiza entrega **al menos una vez**, así que un mismo evento puede
llegar dos veces — por ejemplo si el ACK se pierde al caer un nodo del clúster.
`ControlReintentos` recuerda los `eventId` ya procesados y descarta los
duplicados. El almacenamiento es en memoria: con varias réplicas habría que
moverlo a Redis.

---

## Estructura

```
config/
  RabbitProperties.java        nombres y límites, desde application.yaml
  RabbitTopologyConfig.java    beans Queue, Exchange y Binding por caso de uso
  RabbitSerializationConfig.java  JSON + soporte de Instant
messaging/
  Envelope.java                sobre común a todos los mensajes
  ControlReintentos.java       conteo de intentos e idempotencia
  consumer/
    EmailConsumer.java         dominio notificación
    DispatchConsumer.java      dominio despacho
    ReportConsumer.java        dominio documentos
    DlqMonitor.java            registro y métricas de las tres DLQ
service/
  EmailService.java            lógica de negocio, sin RabbitMQ
  DispatchService.java
  ReportService.java
exception/
  ErrorRecuperableException.java
  ErrorNoRecuperableException.java
controller/
  HealthController.java        /api/health y /api/dlq
```

Los servicios de `service/` no importan nada de AMQP, y las clases de
`config/` no contienen reglas de negocio.

---

## Formato de mensaje

```json
{
  "type": "orden.asignada",
  "eventId": "9f1c2b4e-...",
  "timestamp": "2026-10-17T22:15:03Z",
  "traceId": "req-4471",
  "correlationId": "OT-0042",
  "datos": {
    "ordenId": "OT-0042",
    "destinatario": "cliente@ejemplo.cl"
  }
}
```

Campos de `datos` según el flujo:

- **email** — `destinatario` (obligatorio), `ordenId`
- **dispatch** — `ordenId`, `cuadrilla`, `direccion` (los tres obligatorios)
- **report** — `ordenId`, `documento` (`informe` o `certificado`)

---

## Probar los reintentos y la DLQ

Los tres servicios reconocen un campo opcional `simularFallo` en `datos`, que
existe para demostrar el comportamiento sin esperar a que se caiga un servicio
real:

| Valor | Efecto |
|---|---|
| `recuperable` | Reintenta hasta el límite del flujo, luego va a la DLQ |
| `permanente` | Va directo a la DLQ |
| ausente | Procesa normalmente |

Desde la Management UI (`http://<host>:15672`), publicar en `cmd.direct` con
routing key `email.send`:

```json
{
  "type": "orden.asignada",
  "eventId": "prueba-dlq-1",
  "timestamp": "2026-10-17T22:15:03Z",
  "traceId": "manual",
  "correlationId": "OT-0001",
  "datos": {
    "ordenId": "OT-0001",
    "destinatario": "cliente@ejemplo.cl",
    "simularFallo": "recuperable"
  }
}
```

En el log se ven los tres intentos y la derivación final. `GET /api/dlq`
devuelve el acumulado por cola.

---

## Ejecutar

Requiere el clúster de RabbitMQ levantado (ver el repo `infra`).

```
./mvnw spring-boot:run
```

Queda en el puerto 8082. Las colas y exchanges se crean solos al arrancar si
no existen.

Variables de entorno para apuntar a otro broker:

| Variable | Por defecto |
|---|---|
| `RABBIT_ADDRESSES` | `localhost:5672,localhost:5673` |
| `RABBIT_USER` | `digitalfix` |
| `RABBIT_PASS` | `digitalfix` |

Se listan dos direcciones a propósito: si el primer nodo del clúster cae, la
conexión pasa al segundo sin reiniciar el servicio.

---

## Pendiente

- Integración real con proveedor de correo y push (hoy el envío se registra en
  el log)
- Generación real del PDF
- Mover `ControlReintentos` a almacenamiento compartido si se despliega con
  más de una réplica
- Cola intermedia con TTL para espaciar los reintentos

---

## Nota sobre Spring Boot 4

Dos nombres pueden haber cambiado respecto de Boot 3:

- El starter. Aquí se usa `spring-boot-starter-amqp`. Si Maven no lo
  encuentra, generar un proyecto en Spring Initializr con la dependencia
  **Spring for RabbitMQ** y copiar el nombre exacto que aparezca en su
  `pom.xml`.
- El convertidor JSON. Spring AMQP 4 podría exponerlo como
  `JacksonJsonMessageConverter` en lugar de `Jackson2JsonMessageConverter`. Si
  el import no resuelve en `RabbitSerializationConfig`, es eso: cambia el
  nombre de la clase y del type mapper, la lógica es la misma.

El resto del código no depende de la versión.
