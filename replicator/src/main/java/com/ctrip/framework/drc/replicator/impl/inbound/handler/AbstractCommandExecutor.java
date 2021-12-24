package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.Collections;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:32.
 */
public abstract class AbstractCommandExecutor<V> implements CommandExecutor<V> {

    public AbstractCommandExecutor(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    private CommandHandler<V> commandHandler;

    @Override
    public CommandFuture<V> handle(SimpleObjectPool<NettyClient> simpleObjectPool) {
        CommandFuture<V> commandFuture = new DefaultCommandFuture<>();
        for (String queryString : getCommand()) {
            ServerCommandPacket packet = getPacket(queryString);
            commandFuture = commandHandler.handle(packet, simpleObjectPool);
        }
        return commandFuture;
    }

    public List<String> getCommand() {
        return Collections.singletonList("");
    }

    protected abstract ServerCommandPacket getPacket(String queryString);
}
