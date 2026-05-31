package org.example.callback.dto;

public class DispatchResult {

    private final boolean success;
    private final Integer httpStatus;
    private final String errorMessage;

    private DispatchResult(boolean success, Integer httpStatus, String errorMessage) {
        this.success = success;
        this.httpStatus = httpStatus;
        this.errorMessage = errorMessage;
    }

    public static DispatchResult success(int httpStatus) {
        return new DispatchResult(true, httpStatus, null);
    }

    /** Used when a non-HTTP publish (e.g. Kafka) is acknowledged successfully. */
    public static DispatchResult publishSuccess() {
        return new DispatchResult(true, null, null);
    }

    public static DispatchResult failure(String errorMessage) {
        return new DispatchResult(false, null, errorMessage);
    }

    public static DispatchResult failure(int httpStatus, String errorMessage) {
        return new DispatchResult(false, httpStatus, errorMessage);
    }

    public boolean isSuccess() {
        return success;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
