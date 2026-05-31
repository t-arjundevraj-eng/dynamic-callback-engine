package org.example.callback.dto;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single row polled from a vendor source queue table.
 */
public class RawQueueEvent {

    private final String sourceTableName;
    private final Map<String, Object> fields;

    public RawQueueEvent(String sourceTableName, Map<String, Object> fields) {
        this.sourceTableName = sourceTableName;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public Object getField(String name) {
        if (name == null) {
            return null;
        }
        if (fields.containsKey(name)) {
            return fields.get(name);
        }
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
