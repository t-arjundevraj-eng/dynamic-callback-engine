package org.example.vendor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.example.messaging.VendorEvent;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/vendors")
public class VendorEventController {

    private final VendorEventProducerService producerService;

    public VendorEventController(VendorEventProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/{vendor}/events")
    public VendorEvent send(@PathVariable String vendor, @RequestBody Map<String, Object> fields) {
        return producerService.send(vendor, fields);
    }

    @PostMapping("/{vendor}/events/batch")
    public List<VendorEvent> sendBatch(@PathVariable String vendor, @RequestBody List<Map<String, Object>> events) {
        return events.stream()
                .map(fields -> producerService.send(vendor, fields))
                .collect(Collectors.toList());
    }

    @ExceptionHandler(VendorValidationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> validationError(VendorValidationException ex) {
        return java.util.Collections.singletonMap("error", ex.getMessage());
    }
}
