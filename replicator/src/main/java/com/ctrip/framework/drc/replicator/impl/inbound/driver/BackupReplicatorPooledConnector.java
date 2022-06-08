package com.ctrip.framework.drc.replicator.impl.inbound.driver;

import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.replicator.impl.inbound.BackupReplicatorChannelHandlerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupReplicatorPooledConnector extends ReplicatorPooledConnector {

    public BackupReplicatorPooledConnector(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected ChannelHandlerFactory getChannelHandlerFactory() {
        return new BackupReplicatorChannelHandlerFactory();
    }

    @Override
    protected void postProcessSimpleObjectPool(SimpleObjectPool<NettyClient> simpleObjectPool, boolean notifyConnectionObserver) {
    }
}
