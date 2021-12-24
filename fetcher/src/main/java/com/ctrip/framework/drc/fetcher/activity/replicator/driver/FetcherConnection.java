package com.ctrip.framework.drc.fetcher.activity.replicator.driver;

import com.ctrip.framework.drc.core.driver.AbstractInstanceConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.config.FetcherSlaveConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.command.FetcherBinlogDumpGtidCommandHandler;
import com.ctrip.framework.drc.fetcher.resource.context.NetworkContextResource;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * Created by mingdongli
 * 2019/9/23 下午5:07.
 */
public class FetcherConnection extends AbstractInstanceConnection implements MySQLConnection {

    private NetworkContextResource networkContextResource;

    private ByteBufConverter byteBufConverter;

    public FetcherConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector, NetworkContextResource networkContextResource, ByteBufConverter byteBufConverter) {
        super(mySQLSlaveConfig, eventHandler, connector);
        this.networkContextResource = networkContextResource;
        this.byteBufConverter = byteBufConverter;
    }

    @Override
    protected void doWithSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnectionCode) {
        GtidSet gtidSet = mySQLSlaveConfig.getGtidSet();
        logger.info("[Dump] gtidset is {}", gtidSet.toString());
        CommandFuture<ResultCode> commandFuture = doExecuteCommand(simpleObjectPool);
        commandFuture.addListener(commandFuture1 -> {
            Throwable throwable = commandFuture1.cause();
            if (throwable != null) {
                logger.error("CommandFuture error for {}", mySQLSlaveConfig.getEndpoint(), throwable);
            }
            reconnect(simpleObjectPool, callBack, reconnectionCode);
        });
    }

    private void reconnect(SimpleObjectPool<NettyClient> simpleObjectPool, DumpCallBack callBack, RECONNECTION_CODE reconnection_code) {
        try {
            simpleObjectPool.clear();
        } catch (ObjectPoolException e) {
            logger.error("simpleObjectPool clear", e);
        }

        if (reconnection_code == null) {
            GtidSet gtidSet = networkContextResource.fetchGtidSet();
            logger.info("[Reconnect] using gtidset {}", gtidSet);
            mySQLSlaveConfig.setGtidSet(gtidSet);
        }

        scheduleReconnect(callBack, reconnection_code);
    }

    protected CommandFuture<ResultCode> doExecuteCommand(SimpleObjectPool<NettyClient> simpleObjectPool) {
        FetcherBinlogDumpGtidCommandHandler dumpGtidCommandHandler = new FetcherBinlogDumpGtidCommandHandler(logEventHandler, byteBufConverter);
        ApplierDumpCommandPacket commandPacket = new ApplierDumpCommandPacket(SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        FetcherSlaveConfig slaveConfig = (FetcherSlaveConfig) mySQLSlaveConfig;
        commandPacket.setApplierName(slaveConfig.getApplierName());
        commandPacket.setGtidSet(slaveConfig.getGtidSet());
        commandPacket.setIncludedDbs(slaveConfig.getIncludedDbs());
        commandPacket.setNameFilter(slaveConfig.getNameFilter());
        logger.info("[Filter] applier name is: {}, includeDbs is: {}, name filter is: {}", slaveConfig.getApplierName(), slaveConfig.getIncludedDbs(), slaveConfig.getNameFilter());
        CommandFuture<ResultCode> commandFuture = dumpGtidCommandHandler.handle(commandPacket, simpleObjectPool);
        return commandFuture;
    }

}
