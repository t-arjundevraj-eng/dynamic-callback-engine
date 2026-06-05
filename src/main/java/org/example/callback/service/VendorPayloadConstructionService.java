package org.example.callback.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.example.callback.CallbackValidationException;
import org.example.callback.dto.RawQueueEvent;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.dto.VendorParamDefinition;
import org.example.callback.repository.UserRegistrationJdbcRepository;
import org.example.callback.repository.VendorRoutingLookupRepository;
import org.example.callback.util.LegacyCallbackParamMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Validates operator/pack mappings, loads user-registration params, and builds legacy GET query payloads.
 */
@Service
public class VendorPayloadConstructionService {

    private static final Logger log = LoggerFactory.getLogger(VendorPayloadConstructionService.class);

    private static final String OPERATOR_FIELD = "operator";
    private static final String PACK_FIELD = "pack_name";
    private static final String MSISDN_FIELD = "msisdn";
    private static final String STATUS_FIELD = "status";
    private static final String IP_FIELD = "ip_address";

    private final UserRegistrationJdbcRepository userRegistrationRepository;
    private final VendorRoutingLookupRepository routingLookupRepository;

    public VendorPayloadConstructionService(
            UserRegistrationJdbcRepository userRegistrationRepository,
            VendorRoutingLookupRepository routingLookupRepository) {
        this.userRegistrationRepository = userRegistrationRepository;
        this.routingLookupRepository = routingLookupRepository;
    }

    public Optional<Map<String, Object>> buildPayload(
            RawQueueEvent event,
            ResolvedVendorConfiguration configuration) {
        try {
            validateRouting(event, configuration);
            Map<String, Object> payload = constructLegacyPayload(event, configuration);
            return Optional.of(payload);
        } catch (CallbackValidationException ex) {
            log.warn("Event rejected for vendor={}, circle={}, queue={}: {}",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    configuration.getQueueName(),
                    ex.getMessage());
            return Optional.empty();
        }
    }

    private void validateRouting(RawQueueEvent event, ResolvedVendorConfiguration configuration) {
        String operator = stringValue(event.getField(OPERATOR_FIELD));
        if (!StringUtils.hasText(operator)) {
            throw new CallbackValidationException("Missing operator on source event");
        }
        String operatorKey = routingLookupRepository
                .resolveOperatorKey(
                        operator,
                        configuration.getAllowedOperatorIds(),
                        configuration.getVendorId(),
                        firstNonBlank(stringValue(event.getField("circle")), configuration.getCircle()))
                .orElse(null);
        if (operatorKey == null) {
            throw new CallbackValidationException(
                    "Operator " + operator + " is not allowed for vendor " + configuration.getVendorId()
                            + " (allowed: " + configuration.getAllowedOperatorIds() + ")");
        }

        String packName = stringValue(event.getField(PACK_FIELD));
        if (!StringUtils.hasText(packName)) {
            throw new CallbackValidationException("Missing pack_name on source event");
        }
        String packKey = routingLookupRepository
                .resolvePackKey(packName, configuration.getAllowedPackIds(), configuration.getVendorId())
                .orElse(null);
        if (packKey == null) {
            throw new CallbackValidationException(
                    "Pack " + packName + " is not active for vendor " + configuration.getVendorId()
                            + " (allowed: " + configuration.getAllowedPackIds() + ")");
        }

        validateNotificationStatus(event, configuration.getAllowedNotificationStatuses());
        validateIpIfConfigured(event, configuration.getAllowedIpAddresses());
    }

    private void validateNotificationStatus(RawQueueEvent event, Set<String> allowedStatuses) {
        if (allowedStatuses == null || allowedStatuses.isEmpty()) {
            return;
        }
        String rowStatus = normalizeStatus(event.getField(STATUS_FIELD));
        if (!StringUtils.hasText(rowStatus)) {
            throw new CallbackValidationException("Missing status on source event");
        }
        if (!isAllowedNotificationStatus(rowStatus, allowedStatuses)) {
            throw new CallbackValidationException(
                    "Status " + rowStatus + " is not configured for callback notification");
        }
    }

    /**
     * Production queue tables often store {@code status} as TINYINT(1); JDBC returns boolean.
     * Legacy PRBT compares numeric notification codes (0, 2, 14, ...).
     */
    private static String normalizeStatus(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        }
        if (value instanceof Number) {
            return String.valueOf(((Number) value).intValue());
        }
        String text = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(text)) {
            return "1";
        }
        if ("false".equalsIgnoreCase(text)) {
            return "0";
        }
        return text;
    }

    private static boolean isAllowedNotificationStatus(String rowStatus, Set<String> allowedStatuses) {
        if (allowedStatuses.contains(rowStatus)) {
            return true;
        }
        // Some MySQL drivers surface active subscription rows as true/1 while config lists 0.
        if ("1".equals(rowStatus) && allowedStatuses.contains("0")) {
            return true;
        }
        return false;
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

    private Map<String, Object> constructLegacyPayload(
            RawQueueEvent event,
            ResolvedVendorConfiguration configuration) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();

        putIfPresent(payload, "vendorName", configuration.getVendorName());
        putIfPresent(payload, "circle", firstNonBlank(
                stringValue(event.getField("circle")), configuration.getCircle()));
        putIfPresent(payload, "msisdn", event.getField(MSISDN_FIELD));
        putIfPresent(payload, "amount", event.getField("price_point_charged"));
        putIfPresent(payload, "transactionId", event.getField("transaction_id"));
        putIfPresent(payload, "action", event.getField("action"));
        putIfPresent(payload, "userStatus", normalizeStatus(event.getField(STATUS_FIELD)));
        putIfPresent(payload, "operator", event.getField(OPERATOR_FIELD));
        putIfPresent(payload, "channel", event.getField("channel"));
        putIfPresent(payload, "packName", event.getField(PACK_FIELD));

        String callBackDate = LegacyCallbackParamMapper.formatCallbackDate(event.getField("request_time"));
        putIfPresent(payload, "callBackDate", callBackDate);

        Map<String, String> registrationParams = userRegistrationRepository.findRegistrationParams(
                stringValue(event.getField(OPERATOR_FIELD)),
                stringValue(event.getField(MSISDN_FIELD)),
                stringValue(event.getField(PACK_FIELD)),
                firstNonBlank(stringValue(event.getField("circle")), configuration.getCircle()));

        if (!registrationParams.isEmpty()) {
            log.info("{} userRegParam123  {}", stringValue(event.getField(MSISDN_FIELD)), registrationParams);
        }

        Map<String, Object> valueSources = buildValueSources(event, registrationParams);
        for (VendorParamDefinition definition : configuration.getParamDefinitions()) {
            Object value = resolveParamValue(definition.getSourceField(), valueSources);
            if (value == null) {
                continue;
            }
            String formatted = formatParamValue(definition.getSourceField(), value);
            if (StringUtils.hasText(formatted)) {
                payload.putIfAbsent(definition.getParamKey(), formatted);
            }
        }

        if (payload.isEmpty()) {
            throw new CallbackValidationException("Constructed payload is empty");
        }

        log.debug("{} paramList is: {}", stringValue(event.getField(MSISDN_FIELD)),
                configuration.getParamDefinitions());
        log.debug("{}_CallBackURL query params built with {} entries",
                stringValue(event.getField(MSISDN_FIELD)), payload.size());
        return payload;
    }

    private static Map<String, Object> buildValueSources(
            RawQueueEvent event,
            Map<String, String> registrationParams) {
        Map<String, Object> sources = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : event.getFields().entrySet()) {
            sources.put(entry.getKey(), entry.getValue());
        }
        Object info = event.getField("info");
        if (info != null && StringUtils.hasText(String.valueOf(info))) {
            sources.putIfAbsent("language", info);
        }
        for (Map.Entry<String, String> entry : registrationParams.entrySet()) {
            sources.put(entry.getKey(), entry.getValue());
        }
        return sources;
    }

    private static Object resolveParamValue(String sourceField, Map<String, Object> valueSources) {
        if (!StringUtils.hasText(sourceField)) {
            return null;
        }
        if (valueSources.containsKey(sourceField)) {
            return valueSources.get(sourceField);
        }
        for (Map.Entry<String, Object> entry : valueSources.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(sourceField)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String formatParamValue(String sourceField, Object value) {
        if (value == null) {
            return null;
        }
        if ("start_date".equalsIgnoreCase(sourceField) || "end_date".equalsIgnoreCase(sourceField)) {
            return LegacyCallbackParamMapper.formatDateTimeParam(value);
        }
        return String.valueOf(value).trim();
    }

    private static void putIfPresent(Map<String, Object> payload, String key, Object value) {
        if (value == null) {
            return;
        }
        String text = String.valueOf(value).trim();
        if (!text.isEmpty()) {
            payload.put(key, text);
        }
    }

    private static String firstNonBlank(String primary, String fallback) {
        if (StringUtils.hasText(primary)) {
            return primary.trim();
        }
        return fallback;
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }
}
