package org.example.vendor;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public final class VendorRetryHeaders {

    public static final String RETRY_COUNT = "x-retry-count";

    private VendorRetryHeaders() {
    }

    public static int retryCount(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(RETRY_COUNT);
        if (header == null || header.value() == null) {
            return 0;
        }
        return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
    }

    public static byte[] retryCountBytes(int retryCount) {
        return String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8);
    }
}
