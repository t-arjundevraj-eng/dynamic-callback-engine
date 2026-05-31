package org.example.callback.service;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.example.callback.dto.CallbackHttpMethod;
import org.example.callback.dto.DispatchResult;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Step 4: Asynchronously dispatches constructed payloads to vendor callback URLs via HTTP GET or POST.
 */
@Service
public class VendorCallbackDispatcher {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackDispatcher.class);

    private final RestTemplate restTemplate;
    private final Executor dispatchExecutor;

    public VendorCallbackDispatcher(
            @Qualifier("vendorCallbackRestTemplate") RestTemplate vendorCallbackRestTemplate,
            @Qualifier("vendorCallbackDispatchExecutor") Executor dispatchExecutor) {
        this.restTemplate = vendorCallbackRestTemplate;
        this.dispatchExecutor = dispatchExecutor;
    }

    public CompletableFuture<DispatchResult> dispatchAsync(
            ResolvedVendorConfiguration configuration,
            Map<String, Object> payload) {
        return CompletableFuture.supplyAsync(
                () -> dispatchSync(configuration, payload),
                dispatchExecutor);
    }

    private DispatchResult dispatchSync(ResolvedVendorConfiguration configuration, Map<String, Object> payload) {
        String targetUrl = configuration.getCallbackUrl();
        CallbackHttpMethod method = configuration.getHttpMethod();

        try {
            ResponseEntity<String> response = execute(method, targetUrl, payload);
            int status = response.getStatusCodeValue();
            if (status >= 200 && status < 300) {
                log.info("Vendor callback succeeded vendor={}, circle={}, method={}, url={}, status={}",
                        configuration.getVendorName(),
                        configuration.getCircle(),
                        method,
                        targetUrl,
                        status);
                return DispatchResult.success(status);
            }
            log.warn("Vendor callback non-success vendor={}, circle={}, method={}, url={}, status={}, body={}",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    method,
                    targetUrl,
                    status,
                    truncate(response.getBody()));
            return DispatchResult.failure(status, "HTTP status " + status);
        } catch (RestClientException ex) {
            log.error("Vendor callback failed vendor={}, circle={}, method={}, url={}: {}",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    method,
                    targetUrl,
                    ex.getMessage());
            return DispatchResult.failure(ex.getMessage());
        }
    }

    private ResponseEntity<String> execute(
            CallbackHttpMethod method,
            String targetUrl,
            Map<String, Object> payload) {
        if (method == CallbackHttpMethod.GET) {
            URI uri = UriComponentsBuilder.fromHttpUrl(targetUrl)
                    .queryParams(toQueryParams(payload))
                    .build(true)
                    .toUri();
            return restTemplate.exchange(uri, HttpMethod.GET, HttpEntity.EMPTY, String.class);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<Map<String, Object>>(payload, headers);
        return restTemplate.exchange(targetUrl, HttpMethod.POST, entity, String.class);
    }

    private static MultiValueMap<String, String> toQueryParams(Map<String, Object> payload) {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getValue() != null) {
                params.add(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return params;
    }

    private static String truncate(String body) {
        if (body == null) {
            return null;
        }
        return body.length() > 500 ? body.substring(0, 500) + "..." : body;
    }
}
