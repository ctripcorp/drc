package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by mingdongli
 * 2019/9/10 上午10:38.
 */
public abstract class AbstractQueryCommand<V> extends AbstractMySQLCommand<V>{

    public AbstractQueryCommand(QueryCommandPacket queryCommandPacket, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(queryCommandPacket, clientPool, scheduled);
    }

    @Override
    public SERVER_COMMAND getCommandType() {
        return SERVER_COMMAND.COM_QUERY;
    }
}
