package com.ctrip.framework.drc.console.monitor.delay.impl.driver;

import com.ctrip.framework.drc.core.driver.AbstractMySQLConnector;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.codec.ChannelHandlerFactory;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.activity.replicator.handler.FetcherChannelHandlerFactory;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.PROCESSORS_SIZE;
import static com.ctrip.framework.drc.core.server.utils.ThreadUtils.getThreadName;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-11-27
 */
public class DelayMonitorPooledConnector extends AbstractMySQLConnector implements MySQLConnector {

    public DelayMonitorPooledConnector(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected ChannelHandlerFactory getChannelHandlerFactory() {
        return new FetcherChannelHandlerFactory();
    }

    @Override
    public String getModuleName() {
        return ModuleEnum.CONSOLE.getDescription();
    }

    @Override
    protected NettyClientFactory getNettyClientFactory(String threadPrefix, boolean autoRead) {
        return new DelayMonitorNettyClientFactory(this.endpoint, threadPrefix, autoRead());
    }

    protected ExecutorService getExecutorService() {
        return ThreadUtils.newThreadExecutorWithCoreTimeout(PROCESSORS_SIZE, getThreadName(THREAD_NAME_PREFIX, threadNamePostfix));
    }
}
