package com.ctrip.framework.drc.core.driver;

import com.ctrip.framework.drc.core.server.config.ModuleName;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.google.common.util.concurrent.ListenableFuture;


/**
 *
 * input: ip, port, user, password
 * output: NettyClient
 *
 * 1: connect to mysql server
 * 2: do authentication
 * 3: NettyClient usage:
 *      when sending request, wrap with mysql package protocol
 *      when receiving data, unwrap with mysql package protocol
 *
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface MySQLConnector extends Lifecycle, ModuleName, ConnectionObservable {

    ListenableFuture<SimpleObjectPool<NettyClient>> getConnectPool();

    ListenableFuture<SimpleObjectPool<NettyClient>> getConnectPool(boolean notifyConnectionObserver);

    boolean autoRead();
}
