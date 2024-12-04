package com.ctrip.framework.drc.messenger.activity.monitor;

import com.ctrip.framework.drc.fetcher.activity.monitor.BaseWatchActivity;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskActivity;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;
import com.ctrip.framework.drc.messenger.container.controller.MqServerController;
import com.ctrip.framework.drc.messenger.container.controller.task.WatchKeyedTask;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class WatchActivity extends BaseWatchActivity {
    
    @InstanceConfig(path = "servers")
    public ConcurrentHashMap<String, ? extends FetcherServer> servers;

    @InstanceConfig(path = "container")
    public MqServerContainer container;
    
    protected final ConcurrentHashMap<String, LastLWM> lastLWMHashMap = new ConcurrentHashMap<>();
    
    @Override
    public void loop() {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        Iterator<String> keys = servers.keys().asIterator();
        while(keys.hasNext()) {
            String key = keys.next();
            try {
                WatchKeyedTask watchKeyedTask = new WatchKeyedTask(key, null, container, lastLWMHashMap);
                MqServerController.keyedExecutor.execute(key,watchKeyedTask);
            } catch (Throwable t) {
                logger.info("UNLIKELY - when patrol {}", servers.get(key).getName());
            }
        }
    }

    @Override
    public <U> TaskActivity<Boolean, U> link(TaskActivity<Boolean, U> latter) {
        throw new AssertionError("WatchActivity.link() should not be invoked.");
    }

}
