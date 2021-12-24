package com.ctrip.framework.drc.fetcher.activity.replicator.driver;

import com.ctrip.framework.drc.core.driver.AbstractMySQLConnector;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.FetcherChannelHandlerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * Created by mingdongli
 * 2019/9/23 下午5:18.
 */
public abstract class FetcherPooledConnector extends AbstractMySQLConnector implements MySQLConnector {

    public FetcherPooledConnector(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected ChannelHandlerFactory getChannelHandlerFactory() {
        return new FetcherChannelHandlerFactory();
    }

}
