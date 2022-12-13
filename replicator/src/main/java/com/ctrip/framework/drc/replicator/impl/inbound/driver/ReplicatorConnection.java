package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.AbstractInstanceConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.position.EntryPosition;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.command.handler.BinlogDumpGtidClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.filter.Resettable;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObservable;
import com.ctrip.framework.drc.core.server.observer.uuid.UuidObserver;
import com.ctrip.framework.drc.replicator.impl.inbound.converter.ReplicatorByteBufConverter;
import com.ctrip.framework.drc.replicator.impl.inbound.handler.*;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.StringUtils;

import java.net.SocketException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.isIntegrityTest;

/**
 * Created by mingdongli
 * 2019/9/20 上午11:09.
 */
public class ReplicatorConnection extends AbstractInstanceConnection implements MySQLConnection, UuidObservable {

    private GtidManager gtidManager;

    private List<Observer> observers = Lists.newCopyOnWriteArrayList();

    private SchemaManager schemaManager;

    private String currentUuid;

    public ReplicatorConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector, GtidManager gtidManager, SchemaManager schemaManager) {
        super(mySQLSlaveConfig, eventHandler, connector);
        this.gtidManager = gtidManager;
        this.schemaManager = schemaManager;
        if (eventHandler instanceof UuidObserver) {
            addObserver((Observer) eventHandler);
        }
    }

    @Override
    public void preDump() {
        NamedCallable namedCallable = new UuidFetchTask();
        try {
            namedCallable.call();
        } catch (Exception e) {
            namedCallable.afterException(e);
        }
    }

    protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {
        envSettingAndRegister(simpleObjectPool);

        GtidSet gtidSet = mySQLSlaveConfig.getGtidSet();

        if (RECONNECTION_CODE.MORE_GTID_ERROR == reconnectionCode) {
            EntryPosition entryPosition = fetchExecutedGtidSet(simpleObjectPool);
            GtidSet remoteGtidSet = new GtidSet(entryPosition.getGtid());
            logger.warn("[GtidSet] slave {} more than master {}", gtidSet, remoteGtidSet);
            gtidSet = gtidSet.replaceGtid(remoteGtidSet, mySQLSlaveConfig.getPreviousMaster());
            mySQLSlaveConfig.setGtidSet(gtidSet);
            logger.warn("[GtidSet] replace with {}", gtidSet);
            gtidManager.updateExecutedGtids(gtidSet);
            refreshSchema();
        } else if (RECONNECTION_CODE.PURGED_GTID_REQUIRED == reconnectionCode) {  //fix online bug
            String purgedGtid = fetchPurgedGtidSet(simpleObjectPool);
            gtidSet = new GtidSet(purgedGtid);
            gtidSet = combine(gtidSet, mySQLSlaveConfig.getGtidSet());
            mySQLSlaveConfig.setGtidSet(gtidSet);
            gtidManager.updateExecutedGtids(gtidSet);
        }

        logger.info("[Dump] gtidset is {}", gtidSet.toString());
        CommandFuture<ResultCode> commandFuture = doExecuteCommand(simpleObjectPool, gtidSet, mySQLSlaveConfig.getSlaveId());
        commandFuture.addListener(commandFuture1 -> {
            Throwable throwable = commandFuture1.cause();
            RECONNECTION_CODE reCode = null;
            if (throwable != null) {
                String message = throwable.getMessage();
                if (throwable instanceof SocketException) {
                    logger.error("SocketException {}", message);
                }
                if (message != null && message.contains(RECONNECTION_CODE.MORE_GTID_ERROR.getMessage())) {
                    reCode = RECONNECTION_CODE.MORE_GTID_ERROR;
                } else if (message != null && message.contains(RECONNECTION_CODE.PURGED_GTID_REQUIRED.getMessage())) {
                    reCode = RECONNECTION_CODE.PURGED_GTID_REQUIRED;
                }
            } else {
                logger.info("BinlogDumpGtidCommandExecutor finished and try to reconnect");
            }
            if (getLifecycleState().isStarted()) {
                reconnect(simpleObjectPool, callBack, reCode);
            } else {
                logger.info("[State] is {} and return", getLifecycleState());
            }
        });

    }

    protected GtidSet combine(GtidSet newGtidSet, GtidSet oldGtidSet) {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.mysql.gtid.purge", mySQLSlaveConfig.getRegistryKey());
        GtidSet unionGtidSet = oldGtidSet.union(newGtidSet);
        logger.info("[Combine-GtidSet] for purged gtidset {} and executed gtidset {} is {}", newGtidSet, oldGtidSet, unionGtidSet);
        return unionGtidSet;
    }

    private void refreshSchema() {
        Endpoint endpoint = mySQLSlaveConfig.getEndpoint();
        schemaManager.clone(endpoint);
    }

    private void reconnect(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnection_code) {
        try {
            simpleObjectPool.clear();
            if (logEventHandler instanceof Resettable) {
                ((Resettable) logEventHandler).reset();
            }
        } catch (ObjectPoolException e) {
            logger.error("simpleObjectPool clear", e);
        }

        if (reconnection_code == null) {
            GtidSet gtidSet = gtidManager.getExecutedGtids();
            logger.info("[Reconnect] using gtidset {}", gtidSet);
            mySQLSlaveConfig.setGtidSet(gtidSet);
        }

        scheduleReconnect(callBack, reconnection_code);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.mysql.reconnect", mySQLSlaveConfig.getRegistryKey());
    }

    protected void envSettingAndRegister(SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (isMaster()) {
            updateSettings(simpleObjectPool);
            registerToSlave(simpleObjectPool);
        }
    }

    public void updateSettings(SimpleObjectPool<NettyClient> simpleObjectPool) {
        UpdateClientCommandHandler updateCommandHandler = new UpdateClientCommandHandler();
        UpdateCommandExecutor updateCommandExecutor = new UpdateCommandExecutor(updateCommandHandler);
        updateCommandExecutor.handle(simpleObjectPool);
    }

    protected void registerToSlave(SimpleObjectPool<NettyClient> simpleObjectPool) {
        RegisterSlaveClientCommandHandler registerSlaveCommandHandler = new RegisterSlaveClientCommandHandler();
        RegisterSlaveCommandExecutor registerSlaveCommandExecutor = new RegisterSlaveCommandExecutor(registerSlaveCommandHandler, mySQLSlaveConfig.getEndpoint(), mySQLSlaveConfig.getSlaveId());
        registerSlaveCommandExecutor.handle(simpleObjectPool);
    }

    protected EntryPosition fetchExecutedGtidSet(SimpleObjectPool<NettyClient> simpleObjectPool) {

        QueryClientCommandHandler queryCommandHandler = new QueryClientCommandHandler();
        MasterStatusQueryCommandExecutor queryCommandExecutor = new MasterStatusQueryCommandExecutor(queryCommandHandler);
        CommandFuture<ResultSetPacket> commandFuture = queryCommandExecutor.handle(simpleObjectPool);
        handleFuture(commandFuture);
        ResultSetPacket resultSetPacket = syncResultSetPacket(commandFuture);
        List<String> fields = resultSetPacket.getFieldValues();
        EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
        if (fields.size() > 4) {
            endPosition.setGtid(fields.get(4));
        }

        return endPosition;
    }

    protected String fetchPurgedGtidSet(SimpleObjectPool<NettyClient> simpleObjectPool) {

        QueryClientCommandHandler queryCommandHandler = new QueryClientCommandHandler();
        PurgedGtidQueryCommandExecutor queryCommandExecutor = new PurgedGtidQueryCommandExecutor(queryCommandHandler);
        CommandFuture<ResultSetPacket> commandFuture = queryCommandExecutor.handle(simpleObjectPool);
        handleFuture(commandFuture);
        ResultSetPacket resultSetPacket = syncResultSetPacket(commandFuture);
        List<String> fields = resultSetPacket.getFieldValues();
        return fields.get(1);
    }

    protected String fetchServerUuid(SimpleObjectPool<NettyClient> simpleObjectPool) {

        QueryClientCommandHandler queryCommandHandler = new QueryClientCommandHandler();
        MasterUuidQueryCommandExecutor queryCommandExecutor = new MasterUuidQueryCommandExecutor(queryCommandHandler);
        CommandFuture<ResultSetPacket> commandFuture = queryCommandExecutor.handle(simpleObjectPool);
        handleFuture(commandFuture);
        ResultSetPacket resultSetPacket = syncResultSetPacket(commandFuture);
        List<String> fields = resultSetPacket.getFieldValues();
        if (fields != null && fields.size() == 2) {
            return fields.get(1);
        }
        return null;
    }

    private ResultSetPacket syncResultSetPacket(CommandFuture<ResultSetPacket> commandFuture) {
        ResultSetPacket resultSetPacket;
        try {
            resultSetPacket = commandFuture.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return resultSetPacket;
    }

    @Override
    public void addObserver(Observer observer) {
        if (!observers.contains(observer)) {
            observers.add(observer);
        }
    }

    @Override
    public void removeObserver(Observer observer) {
        observers.remove(observer);
    }

    protected boolean isMaster() {
        return true;
    }

    protected CommandFuture<ResultCode> doExecuteCommand(SimpleObjectPool<NettyClient> simpleObjectPool, GtidSet gtidSet, long id) {
        BinlogDumpGtidClientCommandHandler dumpGtidCommandHandler = new BinlogDumpGtidClientCommandHandler(logEventHandler, new ReplicatorByteBufConverter());
        BinlogDumpGtidCommandExecutor binlogDumpGtidCommandExecutor = new BinlogDumpGtidCommandExecutor(dumpGtidCommandHandler, gtidSet, id);
        return binlogDumpGtidCommandExecutor.handle(simpleObjectPool);
    }

    @VisibleForTesting
    public void setCurrentUuid(String currentUuid) {
        this.currentUuid = currentUuid;
    }

    class UuidFetchTask implements NamedCallable<Boolean> {

        @Override
        public Boolean call() {
            ListenableFuture<SimpleObjectPool<NettyClient>> listenableFuture = connector.getConnectPool();
            Futures.addCallback(listenableFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(SimpleObjectPool<NettyClient> simpleObjectPool) {
                    currentUuid = fetchServerUuid(simpleObjectPool);
                    if (StringUtils.isNotBlank(currentUuid)) {
                        Set<String> previousUuids = gtidManager.getUuids();
                        boolean added = previousUuids.add(currentUuid);
                        logger.info("[Uuid] {} add to previous set {} with res {}", currentUuid, previousUuids, added);
                        if (added) {
                            gtidManager.setUuids(previousUuids);
                            Set<UUID> uuidSet = Sets.newHashSet();
                            previousUuids.stream().forEach(id -> uuidSet.add(UUID.fromString(id)));
                            mySQLSlaveConfig.setUuidSet(uuidSet);
                            for (Observer observer : observers) {
                                if (observer instanceof UuidObserver) {
                                    observer.update(uuidSet, ReplicatorConnection.this);
                                }
                            }
                            logger.info("update [WHITE UUID] set to {}", uuidSet);
                        }
                    }

                    if (isIntegrityTest()) {
                        EntryPosition entryPosition = fetchExecutedGtidSet(simpleObjectPool);
                        GtidSet gtidSet = new GtidSet(entryPosition.getGtid());
                        mySQLSlaveConfig.setGtidSet(gtidSet);
                        gtidManager.updateExecutedGtids(gtidSet);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("[fetchServerUuid] fetchServerUuid error", t);
                }
            });
            return true;
        }

        @Override
        public void afterException(Throwable t) {
            logger.error("preDump {}:{} error", mySQLSlaveConfig.getEndpoint(), mySQLSlaveConfig.getRegistryKey(), t);
        }
    }

    protected <T> void handleFuture(CommandFuture<T> commandFuture) {

    }
}
