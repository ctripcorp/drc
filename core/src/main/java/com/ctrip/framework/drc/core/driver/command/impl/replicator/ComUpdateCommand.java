package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.OKPacket;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by mingdongli
 * 2019/9/10 上午9:25.
 */
public class ComUpdateCommand extends AbstractQueryCommand<OKPacket> {

    public ComUpdateCommand(QueryCommandPacket queryCommandPacket, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(queryCommandPacket, clientPool, scheduled);
    }

    @Override
    protected OKPacket doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
        OKPacket okPacket = new OKPacket();
        return okPacket.read(byteBuf);
    }

}
