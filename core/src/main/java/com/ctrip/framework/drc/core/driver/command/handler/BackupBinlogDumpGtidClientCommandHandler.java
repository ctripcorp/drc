package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComBackupBinlogDumpGtidCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.client.BinlogDumpGtidCommandPacket;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupBinlogDumpGtidClientCommandHandler extends BinlogDumpGtidClientCommandHandler {

    public BackupBinlogDumpGtidClientCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        super(handler, converter);
    }

    @Override
    public CommandFuture<ResultCode> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof BinlogDumpGtidCommandPacket) {
            BinlogDumpGtidCommandPacket binlogDumpGtidCommandPacket = (BinlogDumpGtidCommandPacket) serverCommandPacket;
            ComBackupBinlogDumpGtidCommand dumpGtidCommand = new ComBackupBinlogDumpGtidCommand(binlogDumpGtidCommandPacket, simpleObjectPool, scheduledExecutorService);
            dumpGtidCommand.addObserver(this);
            return execute(dumpGtidCommand);
        }
        return new DefaultCommandFuture<>();
    }
}
