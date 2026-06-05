package org.example.callback.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.util.StringUtils;

/**
 * Maps legacy queue / registration column names to outbound GET query parameter keys.
 */
public final class LegacyCallbackParamMapper {

    private static final Map<String, String> URL_PARAM_KEYS = new HashMap<String, String>();

    static {
        URL_PARAM_KEYS.put("start_date", "startDate");
        URL_PARAM_KEYS.put("end_date", "endDate");
        URL_PARAM_KEYS.put("transaction_id", "transactionId");
        URL_PARAM_KEYS.put("pack_name", "packName");
        URL_PARAM_KEYS.put("price_point_charged", "amount");
        URL_PARAM_KEYS.put("status", "userStatus");
        URL_PARAM_KEYS.put("vendor_name", "vendorName");
        URL_PARAM_KEYS.put("request_time", "callBackDate");
    }

    private LegacyCallbackParamMapper() {
    }

    public static String toUrlParamKey(String sourceField) {
        if (!StringUtils.hasText(sourceField)) {
            return sourceField;
        }
        String trimmed = sourceField.trim();
        if (URL_PARAM_KEYS.containsKey(trimmed)) {
            return URL_PARAM_KEYS.get(trimmed);
        }
        return trimmed;
    }

    public static String formatCallbackDate(Object requestTime) {
        if (requestTime == null) {
            return null;
        }
        if (requestTime instanceof Date) {
            return new SimpleDateFormat("yyyyMMddHHmmss").format((Date) requestTime);
        }
        if (requestTime instanceof java.sql.Timestamp) {
            return new SimpleDateFormat("yyyyMMddHHmmss").format((java.sql.Timestamp) requestTime);
        }
        String raw = String.valueOf(requestTime).trim();
        if (raw.length() >= 19) {
            String digits = raw.replaceAll("[^0-9]", "");
            if (digits.length() >= 14) {
                return digits.substring(0, 14);
            }
        }
        return raw;
    }

    public static String formatDateTimeParam(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((java.sql.Timestamp) value);
        }
        return String.valueOf(value).trim();
    }
}
