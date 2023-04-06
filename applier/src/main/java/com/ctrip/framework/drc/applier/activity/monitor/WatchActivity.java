package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.server.ApplierServer;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.system.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @Author Slight
 * Aug 19, 2020
 */
public class WatchActivity extends AbstractLoopActivity implements TaskSource<Boolean> {

    private final Logger loggerP = LoggerFactory.getLogger("PROGRESS");

    private static long FIRST_LWM_TOLERANCE_TIME = 10 * 60 * 1000; //10 minutes

    private static long LWM_TOLERANCE_TIME = 60 * 1000; //1 minutes

    @InstanceConfig(path = "servers")
    public ConcurrentHashMap<String, ? extends ApplierServer> servers;

    @InstanceConfig(path = "container")
    public ApplierServerContainer container;

    private ExecutorService executorService = ThreadUtils.newCachedThreadPool("WatchActivityRemove");

    public WatchActivity() {
        String binlogPurgeScaleOut = System.getProperty(SystemConfig.APPLIER_LWM_TOLERANCE_TIME);
        if (binlogPurgeScaleOut != null) {
            LWM_TOLERANCE_TIME = Long.parseLong(binlogPurgeScaleOut);
        }
        logger.info("lwm tolerance time is: {}", LWM_TOLERANCE_TIME);
    }

    @Override
    public void loop() {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        Iterator<String> keys = servers.keys().asIterator();
        while(keys.hasNext()) {
            String key = keys.next();
            try {
                ApplierServer server = servers.get(key);
                if (server != null)
                    patrol(key, server);
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

    private HashMap<String, LastLWM> lastLWMHashMap = new HashMap<>();

    public void patrol(String key, ApplierServer server) {
        try {
            long currentLWM = server.getLWM();
            long currentProgress = server.getProgress();
            long bearingTimeMillis = LWM_TOLERANCE_TIME;
            if (currentLWM == 0)
                bearingTimeMillis = FIRST_LWM_TOLERANCE_TIME;
            long currentTimeMillis = System.currentTimeMillis();
            LastLWM lastLWM = lastLWMHashMap.computeIfAbsent(key, k -> new LastLWM(currentLWM, currentProgress, currentTimeMillis));
            if (lastLWM.lwm == currentLWM && lastLWM.progress == currentProgress) {
                if (currentTimeMillis - lastLWM.lastTimeMillis > bearingTimeMillis) {
                    logger.info("lwm does not raise since {}ms with bearing time {}s, going to remove server ({})", lastLWM.lastTimeMillis, bearingTimeMillis / 1000, key);
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("alert", "lwm does not raise for a long time.", 1, 0);
                    removeServer(key);
                }
            } else {
                lastLWMHashMap.put(key, new LastLWM(currentLWM, currentProgress, currentTimeMillis));
                loggerP.info("go ahead ({}): lwm {} progress {}", key, currentLWM, currentProgress);
            }
            if (server.getTransactionTableStatus() == SystemStatus.STOPPED) {
                logger.info("transaction table status is stopped, going to remove server ({})", key);
                removeServer(key);
            }
            if (server.getApplyActivityStatus() == SystemStatus.STOPPED) {
                logger.info("apply activity status is stopped, going to remove server ({})", key);
                removeServer(key);
            }
        } catch (Throwable t) {
        }
    }

    private void removeServer(String key) {
        executorService.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                container.removeServer(key, true);
            } catch (Exception e) {
                logger.error("watch activity remove serve({}) error", key, e);
            }
            container.registerServer(key);
            lastLWMHashMap.remove(key);
            logger.info("watch activity remove serve({}) cost time: {}ms", key, System.currentTimeMillis() - startTime);
        });
    }

    @Override
    public <U> TaskActivity<Boolean, U> link(TaskActivity<Boolean, U> latter) {
        throw new AssertionError("WatchActivity.link() should not be invoked.");
    }

}
