package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.container.controller.ApplierServerController;
import com.ctrip.framework.drc.applier.container.controller.task.WatchKeyedTask;
import com.ctrip.framework.drc.applier.server.ApplierServer;
import com.ctrip.framework.drc.fetcher.system.AbstractLoopActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.TaskActivity;
import com.ctrip.framework.drc.fetcher.system.TaskSource;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class WatchActivity extends AbstractLoopActivity implements TaskSource<Boolean> {
    
    @InstanceConfig(path = "servers")
    public ConcurrentHashMap<String, ? extends ApplierServer> servers;

    @InstanceConfig(path = "container")
    public ApplierServerContainer container;
    
    protected final ConcurrentHashMap<String, LastLWM> lastLWMHashMap = new ConcurrentHashMap<>();
    
    @Override
    public void loop() {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        Iterator<String> keys = servers.keys().asIterator();
        while(keys.hasNext()) {
            String key = keys.next();
            try {
                WatchKeyedTask watchKeyedTask = new WatchKeyedTask(key, null, container, lastLWMHashMap);
                ApplierServerController.keyedExecutor.execute(key,watchKeyedTask);
            } catch (Throwable t) {
                logger.info("UNLIKELY - when patrol {}", servers.get(key).getName());
            }
        }
    }

    public static class LastLWM {
        public final long lwm;
        public final long progress;
        public final long lastTimeMillis;

        public LastLWM(long lwm, long progress, long lastTimeMillis) {
            this.lwm = lwm;
            this.progress = progress;
            this.lastTimeMillis = lastTimeMillis;
        }
    }
    

    @Override
    public <U> TaskActivity<Boolean, U> link(TaskActivity<Boolean, U> latter) {
        throw new AssertionError("WatchActivity.link() should not be invoked.");
    }

}
