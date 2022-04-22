package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.impl.applier.ComApplierBinlogDumpGtidCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * for replicator slave dump binlog from replicator master
 * Created by mingdongli
 * 2019/9/24 上午11:06.
 */
public class DrcBinlogDumpGtidCommandHandler extends BinlogDumpGtidClientCommandHandler {

    public DrcBinlogDumpGtidCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        super(handler, converter);
    }

    @Override
    public CommandFuture<ResultCode> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof ApplierDumpCommandPacket) {
            ApplierDumpCommandPacket commandPacket = (ApplierDumpCommandPacket) serverCommandPacket;
            ComApplierBinlogDumpGtidCommand dumpGtidCommand = new ComApplierBinlogDumpGtidCommand(commandPacket, simpleObjectPool, scheduledExecutorService);
            dumpGtidCommand.addObserver(this);
            return execute(dumpGtidCommand);
        }
        return new DefaultCommandFuture<>();
    }

}