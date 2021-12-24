package com.ctrip.framework.drc.replicator.impl.inbound.handler;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * Created by mingdongli
 * 2019/9/19 下午11:19.
 */
public interface CommandExecutor<V> {

    CommandFuture<V> handle(SimpleObjectPool<NettyClient> simpleObjectPool);
}
