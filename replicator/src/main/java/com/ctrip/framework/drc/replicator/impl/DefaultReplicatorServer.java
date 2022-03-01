package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.enums.DirectionEnum;
import com.ctrip.framework.drc.core.monitor.kpi.DelayMonitorReport;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.impl.AbstractDrcServer;
import com.ctrip.framework.drc.replicator.ReplicatorServer;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.impl.inbound.ReplicatorSlaveServer;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.BackupReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.DefaultFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.FilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.DefaultTransactionFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.BackupEventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.MySQLMasterServer;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.DelayMonitorCommandHandler;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.google.common.collect.Sets;

import java.security.Security;
import java.util.Set;
import java.util.UUID;

/**
 * @author wenchao.meng
 * <p>
 * Aug 22, 2019
 */
public class DefaultReplicatorServer extends AbstractDrcServer implements ReplicatorServer, Destroyable {

    private MySQLMasterServer mySQLMasterServer;

    private ReplicatorSlaveServer replicatorSlaveServer;

    private EventStore eventStore;

    private MySQLSchemaManager schemaManager;

    private ReplicatorConfig replicatorConfig;

    private ReplicatorLogEventHandler logEventHandler;

    private InboundMonitorReport inboundMonitorReport;

    private OutboundMonitorReport outboundMonitorReport;

    private DelayMonitorReport delayMonitorReport;

    private TransactionCache transactionCache;

    private TableFilterConfiguration tableFilterConfiguration;

    public DefaultReplicatorServer(ReplicatorConfig replicatorConfig, MySQLSchemaManager schemaManager, TableFilterConfiguration tableFilterConfiguration) {

        this.replicatorConfig = replicatorConfig;
        this.schemaManager = schemaManager;
        this.tableFilterConfiguration = tableFilterConfiguration;
        String clusterName = replicatorConfig.getRegistryKey();

        MySQLMasterConfig mySQLMasterConfig = replicatorConfig.getMySQLMasterConfig();
        mySQLMasterServer = new MySQLMasterServer(mySQLMasterConfig);

        MySQLSlaveConfig mySQLSlaveConfig = replicatorConfig.getMySQLSlaveConfig();

        MySQLConnector mySQLConnector;
        boolean isMaster = mySQLSlaveConfig.isMaster();
        if (isMaster) {
            mySQLConnector = new ReplicatorPooledConnector(mySQLSlaveConfig.getEndpoint());
        } else {
            mySQLConnector = new BackupReplicatorPooledConnector(mySQLSlaveConfig.getEndpoint());
        }


        replicatorSlaveServer = new ReplicatorSlaveServer(mySQLSlaveConfig, mySQLConnector, schemaManager);

        DefaultMonitorManager delayMonitor = new DefaultMonitorManager();
        eventStore = new FilePersistenceEventStore(schemaManager, clusterName);
        transactionCache = isMaster ? new EventTransactionCache(eventStore, DefaultTransactionFilterChainFactory.createFilterChain())
                : new BackupEventTransactionCache(eventStore, DefaultTransactionFilterChainFactory.createFilterChain());
        schemaManager.setTransactionCache(transactionCache);
        schemaManager.setEventStore(eventStore);

        TrafficEntity trafficEntity = replicatorConfig.getTrafficEntity(DirectionEnum.IN);
        delayMonitorReport = new DelayMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        inboundMonitorReport = new InboundMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        outboundMonitorReport = new OutboundMonitorReport(replicatorConfig.getClusterAppId(), replicatorConfig.getTrafficEntity(DirectionEnum.OUT));
        inboundMonitorReport.setDelayMonitorReport(delayMonitorReport);
        outboundMonitorReport.setDelayMonitorReport(delayMonitorReport);

        logEventHandler = new ReplicatorLogEventHandler(transactionCache, delayMonitor, DefaultFilterChainFactory.createFilterChain(new FilterChainContext(replicatorConfig.getWhiteUUID(), replicatorConfig.getTableNames(), schemaManager, inboundMonitorReport, transactionCache, delayMonitor, clusterName, tableFilterConfiguration)));

        GtidManager gtidManager = eventStore.getGtidManager();
        Set<UUID> uuidSet = replicatorConfig.getWhiteUUID();
        Set<String> uuidStringSet = Sets.newHashSet();
        for (UUID uuid : uuidSet) {
            String uuidString = uuid.toString();
            uuidStringSet.add(uuidString);
        }
        gtidManager.setUuids(uuidStringSet);
        logger.info("[Uuid] update to {} in gtidManager for {}", uuidStringSet, clusterName);
        logEventHandler.addObserver(gtidManager);  // update gtidset in memory
        replicatorSlaveServer.setLogEventHandler(logEventHandler);
        replicatorSlaveServer.setGtidManager(gtidManager);

        mySQLConnector.addObserver(schemaManager);  //init table


    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        setJvmTtlForDns();
        LifecycleHelper.initializeIfPossible(schemaManager);
        LifecycleHelper.initializeIfPossible(eventStore);
        LifecycleHelper.initializeIfPossible(transactionCache);
        LifecycleHelper.initializeIfPossible(replicatorSlaveServer);
        LifecycleHelper.initializeIfPossible(mySQLMasterServer);
        GtidSet gtidSet = eventStore.getGtidManager().getExecutedGtids();
        GtidSet initedGtidSet = replicatorConfig.getGtidSet();
        if ((initedGtidSet != null && !initedGtidSet.getUUIDSets().isEmpty()) && gtidSet.getUUIDSets().isEmpty()) { //first time
            eventStore.getGtidManager().updateExecutedGtids(initedGtidSet);
            replicatorConfig.setGtidSet(initedGtidSet);
            logger.info("init and update gtidSet of {} to {}", replicatorConfig.getRegistryKey(), initedGtidSet);
        } else {
            replicatorConfig.setGtidSet(gtidSet);
            logger.info("init gtidSet of {} to {}", replicatorConfig.getRegistryKey(), gtidSet);
        }
        LifecycleHelper.initializeIfPossible(inboundMonitorReport);
        LifecycleHelper.initializeIfPossible(outboundMonitorReport);
    }

    private void setJvmTtlForDns() {
        logger.info("SecurityManager is null: {}", System.getSecurityManager() == null);
        logger.info("before Security setting, ttl is: {}", Security.getProperty("networkaddress.cache.ttl"));
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        logger.info("after Security setting, ttl is: {}", Security.getProperty("networkaddress.cache.ttl"));
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(schemaManager);
        LifecycleHelper.startIfPossible(eventStore);
        LifecycleHelper.startIfPossible(transactionCache);
        mySQLMasterServer.addCommandHandler(new ApplierRegisterCommandHandler(eventStore.getGtidManager(), eventStore.getFileManager(), outboundMonitorReport));
        mySQLMasterServer.addCommandHandler(new DelayMonitorCommandHandler(logEventHandler, replicatorConfig.getRegistryKey()));
        LifecycleHelper.startIfPossible(replicatorSlaveServer);
        LifecycleHelper.startIfPossible(mySQLMasterServer);
        LifecycleHelper.startIfPossible(inboundMonitorReport);
        LifecycleHelper.startIfPossible(outboundMonitorReport);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        LifecycleHelper.stopIfPossible(inboundMonitorReport);
        LifecycleHelper.stopIfPossible(outboundMonitorReport);
        LifecycleHelper.stopIfPossible(mySQLMasterServer);
        LifecycleHelper.stopIfPossible(replicatorSlaveServer);
        LifecycleHelper.stopIfPossible(transactionCache);
        LifecycleHelper.stopIfPossible(eventStore);
        tableFilterConfiguration.unregister(this.replicatorConfig.getRegistryKey());
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        LifecycleHelper.disposeIfPossible(inboundMonitorReport);
        LifecycleHelper.disposeIfPossible(outboundMonitorReport);
        LifecycleHelper.disposeIfPossible(mySQLMasterServer);
        LifecycleHelper.disposeIfPossible(replicatorSlaveServer);
        LifecycleHelper.disposeIfPossible(transactionCache);
        LifecycleHelper.disposeIfPossible(eventStore);
    }

    @Override
    public void destroy() throws Exception {
        eventStore.destroy();
    }

    @Override
    public Endpoint getUpstreamMaster() {
        return replicatorConfig.getEndpoint();
    }

}
