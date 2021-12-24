package com.ctrip.framework.drc.core.driver.command.handler;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface CommandHandler<V> {

    CommandFuture<V> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool);

    void handle(ServerCommandPacket serverCommandPacket, NettyClient nettyClient);

    SERVER_COMMAND getCommandType();

}
