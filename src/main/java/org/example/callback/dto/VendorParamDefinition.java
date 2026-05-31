package org.example.callback.dto;

public class VendorParamDefinition {

    private String paramKey;
    private String sourceField;
    private boolean required;

    public VendorParamDefinition() {
    }

    public VendorParamDefinition(String paramKey, String sourceField, boolean required) {
        this.paramKey = paramKey;
        this.sourceField = sourceField;
        this.required = required;
    }

    public void setParamKey(String paramKey) {
        this.paramKey = paramKey;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getParamKey() {
        return paramKey;
    }

    public String getSourceField() {
        return sourceField;
    }

    public boolean isRequired() {
        return required;
    }
}
