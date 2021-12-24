package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.command.handler.DrcBinlogDumpGtidCommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.converter.ReplicatorByteBufConverter;
import com.ctrip.framework.drc.replicator.impl.inbound.handler.DrcBinlogDumpGtidCommandExecutor;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupReplicatorConnection extends ReplicatorConnection {

    public BackupReplicatorConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler, MySQLConnector connector, GtidManager gtidManager, SchemaManager schemaManager) {
        super(mySQLSlaveConfig, eventHandler, connector, gtidManager, schemaManager);
    }

    public void preDump() {

    }

    protected boolean isMaster() {
        return false;
    }

    protected CommandFuture<ResultCode> doExecuteCommand(SimpleObjectPool<NettyClient> simpleObjectPool, GtidSet gtidSet, long id) {
        DrcBinlogDumpGtidCommandHandler binlogDumpGtidCommandHandler = new DrcBinlogDumpGtidCommandHandler(logEventHandler, new ReplicatorByteBufConverter());
        DrcBinlogDumpGtidCommandExecutor binlogDumpGtidCommandExecutor = new DrcBinlogDumpGtidCommandExecutor(binlogDumpGtidCommandHandler, gtidSet, id);
        return binlogDumpGtidCommandExecutor.handle(simpleObjectPool);
    }
}
