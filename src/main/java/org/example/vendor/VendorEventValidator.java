package org.example.vendor;

import java.util.List;
import java.util.Map;
import org.example.messaging.VendorEvent;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorTableMetadataRepository;
import org.springframework.stereotype.Component;

@Component
public class VendorEventValidator {

    private final VendorSchemaRegistry schemaRegistry;
    private final VendorTableMetadataRepository tableMetadataRepository;

    public VendorEventValidator(
            VendorSchemaRegistry schemaRegistry,
            VendorTableMetadataRepository tableMetadataRepository
    ) {
        this.schemaRegistry = schemaRegistry;
        this.tableMetadataRepository = tableMetadataRepository;
    }

    public VendorCallbackQueueConfig validate(VendorEvent event) {
        if (event == null) {
            throw new VendorValidationException("Event body is empty");
        }
        if (event.getVendor() == null || event.getVendor().trim().isEmpty()) {
            throw new VendorValidationException("Vendor is missing");
        }
        if (event.getEventId() == null || event.getEventId().trim().isEmpty()) {
            throw new VendorValidationException("Event id is missing");
        }
        if (event.getSchemaVersion() == null || event.getSchemaVersion().trim().isEmpty()) {
            throw new VendorValidationException("Schema version is missing");
        }
        if (event.getReceivedAt() == null) {
            throw new VendorValidationException("Received timestamp is missing");
        }
        VendorCallbackQueueConfig config = schemaRegistry.getRequired(event.getVendor());
        Map<String, Object> fields = event.getFields();
        if (fields == null) {
            throw new VendorValidationException("Fields are missing for " + event.getVendor());
        }
        List<String> requiredFields = tableMetadataRepository.requiredColumns(config.getTableName());
        for (String requiredField : requiredFields) {
            Object value = fields.get(requiredField);
            if (value == null || String.valueOf(value).trim().isEmpty()) {
                throw new VendorValidationException("Missing required field: " + requiredField);
            }
        }
        return config;
    }
}
