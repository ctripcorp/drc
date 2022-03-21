package com.ctrip.framework.drc.console.monitor.delay.server;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorServerConfig;
import com.ctrip.framework.drc.console.monitor.delay.impl.convertor.DelayMonitorByteBufConverter;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorPooledConnector;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.fetcher.activity.event.DumpEventActivity;
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.drc.fetcher.activity.replicator.config.FetcherSlaveConfig;
import com.ctrip.framework.drc.fetcher.activity.replicator.driver.FetcherConnection;
import com.ctrip.framework.drc.fetcher.event.FetcherEvent;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @ClassName DealyMonitorServer
 * @Author haodongPan
 * @Date 2022/3/9 16:27
 * @Version: $
 */
public class DelayMonitorServerHolder extends AbstractLifecycle {
    
    private FetcherSlaveServer server;
    private FetcherSlaveConfig config;
    private PeriodicalUpdateDbTask periodicalUpdateDbTask;
    
    public DelayMonitorServerHolder (DelayMonitorServerConfig delayMonitorServerConfig,PeriodicalUpdateDbTask periodicalUpdateDbTask) {
        new FetcherSlaveServer(delayMonitorServerConfig,new DelayMonitorPooledConnector(config.getEndpoint()),new DelayMonitorByteBufConverter());
    }
    
    

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
    }
}
