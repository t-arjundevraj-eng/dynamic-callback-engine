package org.example.callback.dto;

public enum CallbackHttpMethod {
    GET,
    POST;

    public static CallbackHttpMethod fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return POST;
        }
        return CallbackHttpMethod.valueOf(value.trim().toUpperCase());
    }
}
