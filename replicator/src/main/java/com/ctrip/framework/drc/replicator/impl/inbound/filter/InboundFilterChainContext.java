package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/10/9 上午11:17.
 */
public class InboundFilterChainContext {

    private Set<UUID> whiteUUID = Sets.newConcurrentHashSet();

    private Set<String> tableNames = Sets.newConcurrentHashSet();

    private SchemaManager schemaManager;

    private String registryKey;

    private int applyMode;

    private InboundMonitorReport inboundMonitorReport;

    private TransactionCache transactionCache;

    private DefaultMonitorManager monitorManager;

    private TableFilterConfiguration tableFilterConfiguration;

    private InboundFilterChainContext(Set<UUID> whiteUUID, Set<String> tableNames,
                                     SchemaManager schemaManager, InboundMonitorReport inboundMonitorReport,
                                     TransactionCache transactionCache, DefaultMonitorManager monitorManager,
                                     String registryKey, TableFilterConfiguration tableFilterConfiguration,
                                     int applyMode) {
        setWhiteUUID(whiteUUID);
        setTableNames(tableNames);
        setSchemaManager(schemaManager);
        setInboundMonitorReport(inboundMonitorReport);
        setRegistryKey(registryKey);
        setTransactionCache(transactionCache);
        setDelayMonitor(monitorManager);
        setTableFilterConfiguration(tableFilterConfiguration);
        setApplyMode(applyMode);
    }

    public Set<UUID> getWhiteUUID() {
        return whiteUUID;
    }

    public void setWhiteUUID(Set<UUID> whiteUUID) {
        this.whiteUUID = whiteUUID;
    }

    public int getApplyMode() {
        return applyMode;
    }

    public void setApplyMode(int applyMode) {
        this.applyMode = applyMode;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public InboundMonitorReport getInboundMonitorReport() {
        return inboundMonitorReport;
    }

    public void setInboundMonitorReport(InboundMonitorReport inboundMonitorReport) {
        this.inboundMonitorReport = inboundMonitorReport;
    }

    public TransactionCache getTransactionCache() {
        return transactionCache;
    }

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    public DefaultMonitorManager getMonitorManager() {
        return monitorManager;
    }

    public void setDelayMonitor(DefaultMonitorManager delayMonitor) {
        this.monitorManager = delayMonitor;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    public void setTableFilterConfiguration(TableFilterConfiguration tableFilterConfiguration) {
        this.tableFilterConfiguration = tableFilterConfiguration;
    }

    public void registerBlackTableNameFilter(BlackTableNameFilter filter) {
        tableFilterConfiguration.register(registryKey, filter);
    }

    public static class Builder {

        private Set<UUID> whiteUUID = Sets.newConcurrentHashSet();

        private Set<String> tableNames = Sets.newConcurrentHashSet();

        private SchemaManager schemaManager;

        private String registryKey;

        private int applyMode;

        private InboundMonitorReport inboundMonitorReport;

        private TransactionCache transactionCache;

        private DefaultMonitorManager monitorManager;

        private TableFilterConfiguration tableFilterConfiguration;

        public Builder whiteUUID(Set<UUID> whiteUUID){
            this.whiteUUID = whiteUUID;
            return this;
        }

        public Builder tableNames(Set<String> tableNames){
            this.tableNames = tableNames;
            return this;
        }

        public Builder schemaManager(SchemaManager schemaManager){
            this.schemaManager = schemaManager;
            return this;
        }

        public Builder registryKey(String registryKey){
            this.registryKey = registryKey;
            return this;
        }

        public Builder applyMode(int applyMode){
            this.applyMode = applyMode;
            return this;
        }

        public Builder inboundMonitorReport(InboundMonitorReport inboundMonitorReport){
            this.inboundMonitorReport = inboundMonitorReport;
            return this;
        }

        public Builder transactionCache(TransactionCache transactionCache){
            this.transactionCache = transactionCache;
            return this;
        }

        public Builder monitorManager(DefaultMonitorManager monitorManager){
            this.monitorManager = monitorManager;
            return this;
        }

        public Builder tableFilterConfiguration(TableFilterConfiguration tableFilterConfiguration){
            this.tableFilterConfiguration = tableFilterConfiguration;
            return this;
        }

        public InboundFilterChainContext build() {
            return new InboundFilterChainContext(
                    whiteUUID, tableNames,
                    schemaManager, inboundMonitorReport,
                    transactionCache, monitorManager,
                    registryKey, tableFilterConfiguration,
                    applyMode);
        }

    }
}
