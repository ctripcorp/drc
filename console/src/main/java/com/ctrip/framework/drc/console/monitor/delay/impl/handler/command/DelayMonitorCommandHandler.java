package com.ctrip.framework.drc.console.monitor.delay.impl.handler.command;

import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.converter.ByteBufConverter;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.BinlogDumpGtidClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.impl.delay.DelayMonitorCommand;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacket;
import com.ctrip.framework.drc.core.server.observer.event.LogEventObserver;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * for console dump binlog from replicator
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-02
 */
public class DelayMonitorCommandHandler extends BinlogDumpGtidClientCommandHandler implements CommandHandler<ResultCode>, LogEventObserver {

    public DelayMonitorCommandHandler(LogEventHandler handler, ByteBufConverter converter) {
        super(handler, converter);
    }

    @Override
    public CommandFuture<ResultCode> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof DelayMonitorCommandPacket) {
            DelayMonitorCommandPacket commandPacket = (DelayMonitorCommandPacket) serverCommandPacket;
            DelayMonitorCommand delayMonitorCommand = new DelayMonitorCommand(commandPacket, simpleObjectPool, scheduledExecutorService);
            delayMonitorCommand.addObserver(this);
            return execute(delayMonitorCommand);
        }
        return new DefaultCommandFuture<>();
    }
}
