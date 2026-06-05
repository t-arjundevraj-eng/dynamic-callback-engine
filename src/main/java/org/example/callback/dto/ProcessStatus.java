package org.example.callback.dto;

public final class ProcessStatus {

    public static final String NEW = "0";
    public static final String RETRY = "9";
    public static final String PUBLISHED = "2"; // Standard intermediate state placeholder
    public static final String COMPLETED = "1"; // Standard successful finish status
    public static final String DLQ = "100";     // Matches the numerical failure block found in your DB printout

    private ProcessStatus() {
    }
}