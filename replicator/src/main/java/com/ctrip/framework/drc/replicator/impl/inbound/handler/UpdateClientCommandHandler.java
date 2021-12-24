package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.AbstractClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComUpdateCommand;
import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.packet.server.OKPacket;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * Created by mingdongli
 * 2019/9/18 下午8:31.
 */
public class UpdateClientCommandHandler extends AbstractClientCommandHandler<OKPacket> implements CommandHandler<OKPacket> {

    @Override
    public CommandFuture<OKPacket> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof QueryCommandPacket) {
            QueryCommandPacket queryCommandPacket = (QueryCommandPacket) serverCommandPacket;
            ComUpdateCommand comUpdateCommand = new ComUpdateCommand(queryCommandPacket, simpleObjectPool, scheduledExecutorService);
            return execute(comUpdateCommand);
        }
        return new DefaultCommandFuture<>();
    }
}
