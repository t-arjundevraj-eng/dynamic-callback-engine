package org.example.callback.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.example.callback.CallbackValidationException;
import org.example.callback.dto.RawQueueEvent;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.dto.VendorParamDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Step 3: Validates operator/pack mappings and builds the outbound callback payload.
 */
@Service
public class VendorPayloadConstructionService {

    private static final Logger log = LoggerFactory.getLogger(VendorPayloadConstructionService.class);

    private static final String OPERATOR_FIELD = "operator_id";
    private static final String PACK_FIELD = "pack_id";
    private static final String IP_FIELD = "ip_address";

    public Optional<Map<String, Object>> buildPayload(
            RawQueueEvent event,
            ResolvedVendorConfiguration configuration) {
        try {
            validateRouting(event, configuration);
            Map<String, Object> payload = constructPayload(event, configuration);
            return Optional.of(payload);
        } catch (CallbackValidationException ex) {
            log.debug("Event rejected for vendor={}, circle={}, queue={}: {}",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    configuration.getQueueName(),
                    ex.getMessage());
            return Optional.empty();
        }
    }

    private void validateRouting(RawQueueEvent event, ResolvedVendorConfiguration configuration) {
        String operatorId = stringValue(event.getField(OPERATOR_FIELD));
        if (!StringUtils.hasText(operatorId)) {
            throw new CallbackValidationException("Missing operator_id on source event");
        }
        if (!configuration.getAllowedOperatorIds().contains(operatorId)) {
            throw new CallbackValidationException(
                    "Operator " + operatorId + " is not allowed for vendor " + configuration.getVendorId());
        }

        String packId = stringValue(event.getField(PACK_FIELD));
        if (!StringUtils.hasText(packId)) {
            throw new CallbackValidationException("Missing pack_id on source event");
        }
        if (!configuration.getAllowedPackIds().contains(packId)) {
            throw new CallbackValidationException(
                    "Pack " + packId + " is not active for vendor " + configuration.getVendorId());
        }

        validateIpIfConfigured(event, configuration.getAllowedIpAddresses());
    }

    private void validateIpIfConfigured(RawQueueEvent event, Set<String> allowedIps) {
        if (allowedIps == null || allowedIps.isEmpty()) {
            return;
        }
        String eventIp = stringValue(event.getField(IP_FIELD));
        if (!StringUtils.hasText(eventIp)) {
            return;
        }
        if (!allowedIps.contains(eventIp)) {
            throw new CallbackValidationException("IP " + eventIp + " is not allowed for this vendor");
        }
    }

    private Map<String, Object> constructPayload(
            RawQueueEvent event,
            ResolvedVendorConfiguration configuration) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();

        for (VendorParamDefinition definition : configuration.getParamDefinitions()) {
            Object value = event.getField(definition.getSourceField());
            if (value == null && definition.isRequired()) {
                throw new CallbackValidationException(
                        "Required source field missing: " + definition.getSourceField());
            }
            if (value != null) {
                payload.put(definition.getParamKey(), value);
            }
        }

        if (StringUtils.hasText(configuration.getCircle())) {
            payload.putIfAbsent("circle", configuration.getCircle());
        }
        if (StringUtils.hasText(configuration.getChannelUrl())) {
            payload.putIfAbsent("channelUrl", configuration.getChannelUrl());
        }

        if (payload.isEmpty()) {
            throw new CallbackValidationException("Constructed payload is empty");
        }
        return payload;
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}
