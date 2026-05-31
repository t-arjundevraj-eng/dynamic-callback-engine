package org.example.producer;

import javax.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/load")
public class LoadController {

    private final LoadProducerService producerService;

    public LoadController(LoadProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/start")
    public LoadJobStatus start(@Valid @RequestBody LoadRequest request) {
        return producerService.start(request);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<LoadJobStatus> status(@PathVariable String jobId) {
        try {
            return ResponseEntity.ok(producerService.status(jobId));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.notFound().build();
        }
    }
}
