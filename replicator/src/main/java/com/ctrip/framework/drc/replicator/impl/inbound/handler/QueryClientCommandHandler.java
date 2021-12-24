package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.AbstractClientCommandHandler;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComQueryCommand;
import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * Created by mingdongli
 * 2019/9/18 下午7:41.
 */
public class QueryClientCommandHandler extends AbstractClientCommandHandler<ResultSetPacket> implements CommandHandler<ResultSetPacket> {

    @Override
    public CommandFuture<ResultSetPacket> handle(ServerCommandPacket serverCommandPacket, SimpleObjectPool<NettyClient> simpleObjectPool) {
        if (serverCommandPacket instanceof QueryCommandPacket) {
            QueryCommandPacket queryCommandPacket = (QueryCommandPacket) serverCommandPacket;
            ComQueryCommand comQueryCommand = new ComQueryCommand(queryCommandPacket, simpleObjectPool, scheduledExecutorService);
            return comQueryCommand.execute();
        }
        return new DefaultCommandFuture<>();
    }

}
