package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * act as client to send command
 * Created by mingdongli
 * 2019/9/18 下午7:30.
 */
public abstract class AbstractClientCommandHandler<V> implements CommandHandler<V> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    public void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient) {}

    public SERVER_COMMAND getCommandType() {
        return null;
    }

    protected CommandFuture<V> execute(Command<V> command) {
        return command.execute();
    }

}
