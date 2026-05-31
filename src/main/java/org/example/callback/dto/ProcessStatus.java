package org.example.callback.dto;

public final class ProcessStatus {

    public static final String NEW = "NEW";
    public static final String RETRY = "RETRY";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String COMPLETED = "COMPLETED";
    public static final String DLQ = "DLQ";

    private ProcessStatus() {
    }
}
