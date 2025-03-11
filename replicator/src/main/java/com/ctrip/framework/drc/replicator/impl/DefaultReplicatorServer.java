package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.codec.FileCheck;
import com.ctrip.framework.drc.core.driver.config.InstanceStatus;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.enums.DirectionEnum;
import com.ctrip.framework.drc.core.monitor.kpi.DelayMonitorReport;
import com.ctrip.framework.drc.core.monitor.kpi.InboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.config.replicator.MySQLMasterConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorDetailInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.server.impl.AbstractDrcServer;
import com.ctrip.framework.drc.replicator.ReplicatorServer;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfiguration;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.ReplicatorSlaveServer;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.BackupReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorPooledConnector;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandler;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.EventFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.InboundFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.TransactionFilterChainFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.BackupEventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManager;
import com.ctrip.framework.drc.replicator.impl.oubound.MySQLMasterServer;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScannerManager;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.DelayMonitorCommandHandler;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.HeartBeatCommandHandler;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileCheck;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private BinlogScannerManager scannerManager;

    public DefaultReplicatorServer(ReplicatorConfig replicatorConfig, MySQLSchemaManager schemaManager, TableFilterConfiguration tableFilterConfiguration, UuidOperator uuidOperator) {

        this.replicatorConfig = replicatorConfig;
        this.schemaManager = schemaManager;
        this.tableFilterConfiguration = tableFilterConfiguration;
        String clusterName = replicatorConfig.getRegistryKey();

        MySQLMasterConfig mySQLMasterConfig = replicatorConfig.getMySQLMasterConfig();
        mySQLMasterServer = new MySQLMasterServer(mySQLMasterConfig);

        MySQLSlaveConfig mySQLSlaveConfig = replicatorConfig.getMySQLSlaveConfig();
        boolean isMaster = mySQLSlaveConfig.isMaster();

        DefaultMonitorManager delayMonitor = new DefaultMonitorManager(clusterName);
        int applyMode = mySQLSlaveConfig.getApplyMode();
        eventStore = new FilePersistenceEventStore(schemaManager, uuidOperator, replicatorConfig);
        InboundFilterChainContext transactionContext = new InboundFilterChainContext.Builder().applyMode(applyMode).build();
        transactionCache = isMaster ? new EventTransactionCache(eventStore, new TransactionFilterChainFactory().createFilterChain(transactionContext), clusterName)
                : new BackupEventTransactionCache(eventStore, new TransactionFilterChainFactory().createFilterChain(transactionContext), clusterName);
        schemaManager.setTransactionCache(transactionCache);
        schemaManager.setEventStore(eventStore);

        FileCheck fileCheck = new DefaultFileCheck(clusterName, eventStore.getFileManager(), mySQLSlaveConfig.getEndpoint());
        MySQLConnector mySQLConnector = isMaster ? new ReplicatorPooledConnector(mySQLSlaveConfig.getEndpoint(), fileCheck)
                : new BackupReplicatorPooledConnector(mySQLSlaveConfig.getEndpoint());

        replicatorSlaveServer = new ReplicatorSlaveServer(mySQLSlaveConfig, mySQLConnector, schemaManager);// mysql  binlog dump Server

        TrafficEntity trafficEntity = replicatorConfig.getTrafficEntity(DirectionEnum.IN);
        delayMonitorReport = new DelayMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        inboundMonitorReport = new InboundMonitorReport(replicatorConfig.getClusterAppId(), trafficEntity);
        outboundMonitorReport = new OutboundMonitorReport(replicatorConfig.getClusterAppId(), replicatorConfig.getTrafficEntity(DirectionEnum.OUT));
        inboundMonitorReport.setDelayMonitorReport(delayMonitorReport);
        outboundMonitorReport.setDelayMonitorReport(delayMonitorReport);

        GtidManager gtidManager = eventStore.getGtidManager();
        logEventHandler = new ReplicatorLogEventHandler(transactionCache, delayMonitor, new EventFilterChainFactory().createFilterChain(
                new InboundFilterChainContext.Builder()
                        .whiteUUID(gtidManager.toUUIDSet())
                        .tableNames(replicatorConfig.getTableNames())
                        .schemaManager(schemaManager)
                        .inboundMonitorReport(inboundMonitorReport)
                        .transactionCache(transactionCache)
                        .monitorManager(delayMonitor)
                        .registryKey(clusterName)
                        .tableFilterConfiguration(tableFilterConfiguration)
                        .master(isMaster)
                        .applyMode(applyMode).build()));

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

        ApplierRegisterCommandHandler applierRegisterCommandHandler = new ApplierRegisterCommandHandler(eventStore.getGtidManager(), eventStore.getFileManager(), outboundMonitorReport, replicatorConfig);
        scannerManager = applierRegisterCommandHandler.getBinlogScannerManager();
        mySQLMasterServer.addCommandHandler(applierRegisterCommandHandler);
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

    public ReplicatorInfoDto info() {
        ReplicatorInfoDto replicatorInfoDto = new ReplicatorInfoDto();
        replicatorInfoDto.setRegistryKey(this.replicatorConfig.getRegistryKey());
        InstanceStatus instanceStatus = InstanceStatus.getInstanceStatus(replicatorConfig.getStatus());
        replicatorInfoDto.setMaster(instanceStatus == InstanceStatus.ACTIVE);
        replicatorInfoDto.setIp(this.replicatorConfig.getMySQLMasterConfig().getIp());
        replicatorInfoDto.setPort(null);
        replicatorInfoDto.setApplierPort(this.replicatorConfig.getApplierPort());
        replicatorInfoDto.setUpstreamMasterIp(this.replicatorConfig.getMySQLSlaveConfig().getEndpoint().getHost());
        return replicatorInfoDto;
    }

    public ReplicatorDetailInfoDto detailInfo() {
        ReplicatorDetailInfoDto replicatorDetailInfoDto = new ReplicatorDetailInfoDto();
        GtidManager gtidManager = this.eventStore.getGtidManager();
        FileManager fileManager = this.eventStore.getFileManager();
        String firstFile = fileManager.getFirstLogFile().getName();
        String latestFile = fileManager.getCurrentLogFileName();
        replicatorDetailInfoDto.setRegistryKey(this.replicatorConfig.getRegistryKey());
        replicatorDetailInfoDto.setOldestBinlogFile(firstFile);
        replicatorDetailInfoDto.setLatestBinlogFile(latestFile);
        replicatorDetailInfoDto.setPurgedGtidSet(gtidManager.getPurgedGtids().toString());
        replicatorDetailInfoDto.setExecutedGtidSet(gtidManager.getExecutedGtids().toString());

        // scanner info
        Map<ConsumeType, List<BinlogScanner>> senderSizeByType = scannerManager.getScanners().stream().collect(Collectors.groupingBy(BinlogScanner::getConsumeType));
        for (Map.Entry<ConsumeType, List<BinlogScanner>> entry : senderSizeByType.entrySet()) {
            List<BinlogScanner> scanners = entry.getValue();
            for (BinlogScanner scanner : scanners) {
                ReplicatorDetailInfoDto.ScannerDto scannerDto = new ReplicatorDetailInfoDto.ScannerDto();
                BinlogPosition binlogPosition = scanner.getBinlogPosition();
                scannerDto.setBinlogPosition(binlogPosition.toString());
                scannerDto.setConsumeType(scanner.getConsumeType().name());
                scannerDto.setCurrentFile(scanner.getCurrentSendingFileName());
                List<ReplicatorDetailInfoDto.SenderDto> senders = scanner.getSenders().stream().map(sender -> {
                    ReplicatorDetailInfoDto.SenderDto senderDto = new ReplicatorDetailInfoDto.SenderDto(sender.getApplierName());
                    senderDto.setBinlogPosition(sender.getBinlogPosition().toString());
                    return senderDto;
                }).collect(Collectors.toList());
                scannerDto.setSenders(senders);
                replicatorDetailInfoDto.addScanner(scannerDto);
            }
        }
        return replicatorDetailInfoDto;
    }

    @VisibleForTesting
    protected ReplicatorLogEventHandler getLogEventHandler() {
        return logEventHandler;
    }
}
