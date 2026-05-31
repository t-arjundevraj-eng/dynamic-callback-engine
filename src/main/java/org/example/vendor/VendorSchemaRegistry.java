package org.example.vendor;

import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.springframework.stereotype.Component;

@Component
public class VendorSchemaRegistry {

    private final VendorCallbackQueueConfigRepository configRepository;

    public VendorSchemaRegistry(VendorCallbackQueueConfigRepository configRepository) {
        this.configRepository = configRepository;
    }

    public VendorCallbackQueueConfig getRequired(String vendorName) {
        VendorCallbackQueueConfig config = configRepository.findActiveByVendorName(vendorName);
        if (config == null) {
            throw new VendorValidationException("Unknown or inactive vendor: " + vendorName);
        }
        return config;
    }
}
