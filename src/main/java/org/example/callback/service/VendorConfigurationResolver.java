package org.example.callback.service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.example.callback.config.VendorCallbackProperties;
import org.example.callback.dto.CallbackHttpMethod;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.dto.VendorConfigurationRow;
import org.example.callback.dto.VendorParamDefinition;
import org.example.callback.repository.VendorCallbackSourceTableProvisioner;
import org.example.callback.repository.VendorConfigurationJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Service;

/**
 * Step 1: Joins normalized vendor tables and builds fully validated in-memory routing rules.
 */
@Service
public class VendorConfigurationResolver {

    private static final Logger log = LoggerFactory.getLogger(VendorConfigurationResolver.class);

    private final VendorConfigurationJdbcRepository configurationRepository;
    private final VendorCallbackSourceTableProvisioner sourceTableProvisioner;
    private final VendorCallbackProperties properties;
    private final AtomicReference<List<ResolvedVendorConfiguration>> cache =
            new AtomicReference<List<ResolvedVendorConfiguration>>(new ArrayList<ResolvedVendorConfiguration>());

    public VendorConfigurationResolver(
            VendorConfigurationJdbcRepository configurationRepository,
            VendorCallbackSourceTableProvisioner sourceTableProvisioner,
            VendorCallbackProperties properties) {
        this.configurationRepository = configurationRepository;
        this.sourceTableProvisioner = sourceTableProvisioner;
        this.properties = properties;
    }

    public List<ResolvedVendorConfiguration> getResolvedConfigurations() {
        return cache.get();
    }

    @PostConstruct
    public void loadOnStartup() {
        if (properties.isEnabled()) {
            refresh();
        }
    }

    public void refresh() {
        List<VendorConfigurationRow> rows = configurationRepository.findResolvedVendorRows();
        List<ResolvedVendorConfiguration> resolved = new ArrayList<ResolvedVendorConfiguration>(rows.size());

        for (VendorConfigurationRow row : rows) {
            try {
                sourceTableProvisioner.ensureSourceTable(
                        row.getVendorId(), row.getSourceTableName(), row.getCircle());
                resolved.add(buildResolvedConfiguration(row));
            } catch (Exception ex) {
                log.warn("Skipping invalid vendor configuration vendorId={}, circle={}, queueId={}: {}",
                        row.getVendorId(), row.getCircle(), row.getQueueId(), ex.getMessage());
            }
        }

        cache.set(resolved);
        log.info("Refreshed {} resolved vendor callback configuration(s)", resolved.size());
    }

    private ResolvedVendorConfiguration buildResolvedConfiguration(VendorConfigurationRow row) {
        int vendorId = row.getVendorId();
        String circle = row.getCircle();

        Set<String> operators = new LinkedHashSet<String>(
                configurationRepository.findOperatorIds(vendorId));
        Set<String> packs = new LinkedHashSet<String>(
                configurationRepository.findActivePackIds(vendorId));
        Set<String> ips = new LinkedHashSet<String>(
                configurationRepository.findAllowedIpAddresses(vendorId));
        List<VendorParamDefinition> params =
                configurationRepository.findParamDefinitions(vendorId, circle);

        if (operators.isEmpty()) {
            throw new IllegalStateException("No operator mappings for vendor " + vendorId);
        }
        if (packs.isEmpty()) {
            throw new IllegalStateException("No active packs for vendor " + vendorId);
        }
        if (params.isEmpty()) {
            throw new IllegalStateException("No parameter schema for vendor " + vendorId + " circle " + circle);
        }

        int fetchSize = row.getFetchSize() > 0 ? row.getFetchSize() : 100;
        long sleepMs = row.getProducerSleepTimeMs() > 0
                ? row.getProducerSleepTimeMs()
                : properties.getDefaultProducerSleepMs();

        return new ResolvedVendorConfiguration(
                vendorId,
                row.getVendorName(),
                circle,
                row.getCallbackUrl(),
                row.getChannelUrl(),
                CallbackHttpMethod.fromString(row.getHttpMethod()),
                row.getQueueId(),
                row.getQueueName(),
                row.getSourceTableName(),
                fetchSize,
                sleepMs,
                row.getMaxRetryCount(),
                operators,
                packs,
                ips,
                params
        );
    }
}
