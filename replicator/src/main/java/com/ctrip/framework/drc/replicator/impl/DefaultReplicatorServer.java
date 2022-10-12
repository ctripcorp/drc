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
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.ReplicatorSlaveServer;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.BackupReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.DefaultTransactionFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.BackupEventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.MySQLMasterServer;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.DelayMonitorCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.HeartBeatCommandHandler;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.utils.VisibleForTesting;

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

    public DefaultReplicatorServer(ReplicatorConfig replicatorConfig, MySQLSchemaManager schemaManager, TableFilterConfiguration tableFilterConfiguration, UuidOperator uuidOperator) {

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

        DefaultMonitorManager delayMonitor = new DefaultMonitorManager(clusterName);
        eventStore = new FilePersistenceEventStore(schemaManager, uuidOperator, replicatorConfig);
        transactionCache = isMaster ? new EventTransactionCache(eventStore, DefaultTransactionFilterChainFactory.createFilterChain(mySQLSlaveConfig.getApplyMode()))
                : new BackupEventTransactionCache(eventStore, DefaultTransactionFilterChainFactory.createFilterChain(mySQLSlaveConfig.getApplyMode()));
        schemaManager.setTransactionCache(transactionCache);
        schemaManager.setEventStore(eventStore);

        TrafficEntity trafficEntity = replicatorConfig.getTrafficEntity(DirectionEnum.IN);
        delayMonitorReport = new DelayMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        inboundMonitorReport = new InboundMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        outboundMonitorReport = new OutboundMonitorReport(replicatorConfig.getClusterAppId(), replicatorConfig.getTrafficEntity(DirectionEnum.OUT));
        inboundMonitorReport.setDelayMonitorReport(delayMonitorReport);
        outboundMonitorReport.setDelayMonitorReport(delayMonitorReport);

        GtidManager gtidManager = eventStore.getGtidManager();
        logEventHandler = new ReplicatorLogEventHandler(transactionCache, delayMonitor, new InboundFilterChainFactory().createFilterChain(
                new InboundFilterChainContext(gtidManager.toUUIDSet(), replicatorConfig.getTableNames(), schemaManager, inboundMonitorReport,
                        transactionCache, delayMonitor, clusterName, tableFilterConfiguration, mySQLSlaveConfig.getApplyMode())));

        logEventHandler.addObserver(gtidManager);  // update gtidset in memory
        replicatorSlaveServer.setLogEventHandler(logEventHandler);
        replicatorSlaveServer.setGtidManager(gtidManager);

        mySQLConnector.addObserver(schemaManager);  //init table
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        LifecycleHelper.initializeIfPossible(schemaManager);
        LifecycleHelper.initializeIfPossible(eventStore);
        LifecycleHelper.initializeIfPossible(transactionCache);
        LifecycleHelper.initializeIfPossible(replicatorSlaveServer);

        mySQLMasterServer.addCommandHandler(new ApplierRegisterCommandHandler(eventStore.getGtidManager(), eventStore.getFileManager(), outboundMonitorReport, replicatorConfig));
        mySQLMasterServer.addCommandHandler(new DelayMonitorCommandHandler(logEventHandler, replicatorConfig.getRegistryKey()));
        mySQLMasterServer.addCommandHandler(new HeartBeatCommandHandler(replicatorConfig.getRegistryKey()));
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

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LifecycleHelper.startIfPossible(schemaManager);
        LifecycleHelper.startIfPossible(eventStore);
        LifecycleHelper.startIfPossible(transactionCache);
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

    @VisibleForTesting
    protected ReplicatorLogEventHandler getLogEventHandler() {
        return logEventHandler;
    }
}
