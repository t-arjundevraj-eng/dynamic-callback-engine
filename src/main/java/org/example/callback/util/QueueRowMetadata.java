package org.example.callback.util;

import java.util.Map;
import org.example.callback.dto.PendingQueueRow;

public final class QueueRowMetadata {

    // Fixed from "id" to match your actual table column name 'request_id'
    private static final String ID = "request_id"; 
    private static final String RETRY_COUNT = "retry_count";

    private QueueRowMetadata() {
    }

    public static PendingQueueRow fromRow(Map<String, Object> row) {
        String id = requireString(row, ID);
        int retryCount = intValue(row, RETRY_COUNT, 0);
        return new PendingQueueRow(id, retryCount, row);
    }

    private static String requireString(Map<String, Object> row, String column) {
        Object value = getIgnoreCase(row, column);
        if (value == null) {
            throw new IllegalArgumentException("Source row is missing required column: " + column);
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("Source row has empty required column: " + column);
        }
        return text;
    }

    private static int intValue(Map<String, Object> row, String column, int defaultValue) {
        Object value = getIgnoreCase(row, column);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private static Object getIgnoreCase(Map<String, Object> row, String name) {
        if (row.containsKey(name)) {
            return row.get(name);
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }
}