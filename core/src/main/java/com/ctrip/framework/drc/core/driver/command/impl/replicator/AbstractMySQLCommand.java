package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.MySQLCommand;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.AbstractNettyRequestResponseCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public abstract class AbstractMySQLCommand<V> extends AbstractNettyRequestResponseCommand<V> implements MySQLCommand<V> {

    private ServerCommandPacket commandPacket;

    public AbstractMySQLCommand(ServerCommandPacket commandPacket, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.commandPacket = commandPacket;
    }

    @Override
    public ByteBuf getRequest() {
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer();
        try {
            commandPacket.write(byteBuf);
        } catch (IOException e) {
            getLogger().error("decode error for {}", getName(), e);
        }
        return byteBuf;
    }

    @Override
    public void clientClosed(NettyClient nettyClient) {
        super.clientClosed(nettyClient);
        Throwable throwable = future().cause();
        if (throwable != null) {
            getLogger().error("{} clientClosed", getClass().getSimpleName(), throwable);
        }
    }

    @Override
    public String getName() {
        return getCommandType().name();
    }

}
