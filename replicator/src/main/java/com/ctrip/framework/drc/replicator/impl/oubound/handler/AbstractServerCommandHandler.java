package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/9/21 下午11:17
 */
public abstract class AbstractServerCommandHandler<V> implements CommandHandler<V> , Disposable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public abstract SERVER_COMMAND getCommandType();

    @Override
    public CommandFuture<V> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        return null;
    }

}
