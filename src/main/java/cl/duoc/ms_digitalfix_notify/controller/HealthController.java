package cl.duoc.ms_digitalfix_notify.controller;

import java.time.LocalDateTime;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.ms_digitalfix_notify.messaging.consumer.DlqMonitor;

/**
 * Sonda de estado y metricas de DLQ.
 *
 * El servicio no expone API de negocio: su entrada son las colas. Estos dos
 * endpoints existen para operacion, y /api/dlq sirve ademas para mostrar en
 * la presentacion cuantos mensajes terminaron muertos por cola.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    private final DlqMonitor dlqMonitor;

    @Autowired
    public HealthController(DlqMonitor dlqMonitor) {
        this.dlqMonitor = dlqMonitor;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "servicio", "ms-digitalfix-notify",
                "estado", "UP",
                "timestamp", LocalDateTime.now()));
    }

    @GetMapping("/dlq")
    public ResponseEntity<Map<String, Long>> dlq() {
        return ResponseEntity.ok(dlqMonitor.metricas());
    }
}
