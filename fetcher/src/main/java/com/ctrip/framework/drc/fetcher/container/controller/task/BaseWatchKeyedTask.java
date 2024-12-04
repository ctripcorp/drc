package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.activity.monitor.BaseWatchActivity;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import com.ctrip.framework.drc.fetcher.system.qconfig.FetcherDynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by shiruixin
 * 2024/11/20 15:30
 */
public class BaseWatchKeyedTask extends FetcherKeyedTask {

    protected final Logger loggerP = LoggerFactory.getLogger("PROGRESS");

    protected ConcurrentHashMap<String, BaseWatchActivity.LastLWM> lastLWMHashMap;

    public BaseWatchKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer,
                              ConcurrentHashMap<String, BaseWatchActivity.LastLWM> lastLWMHashMap) {
        super(registryKey, applierConfig, serverContainer);
        this.lastLWMHashMap = lastLWMHashMap;
    }


    @Override
    public void doExecute() throws Throwable {
        patrol(registryKey, serverContainer.getServer(registryKey));
    }

    // catch throwable to avoid retry, wait next round
    protected void patrol(String key, FetcherServer server) {
        try {
            long currentLWM = server.getLWM();
            long currentProgress = server.getProgress();
            long bearingTimeMillis = FetcherDynamicConfig.getInstance().getLwmToleranceTime();
            if (currentLWM == 0)
                bearingTimeMillis = FetcherDynamicConfig.getInstance().getFirstLwmToleranceTime();
            long currentTimeMillis = System.currentTimeMillis();
            BaseWatchActivity.LastLWM lastLWM = lastLWMHashMap.computeIfAbsent(key, k -> new BaseWatchActivity.LastLWM(currentLWM, currentProgress, currentTimeMillis));
            if (lastLWM.lwm == currentLWM && lastLWM.progress == currentProgress) {
                if (currentTimeMillis - lastLWM.lastTimeMillis > bearingTimeMillis) {
                    logger.info("lwm does not raise since {}ms with bearing time {}s, going to remove server ({})", lastLWM.lastTimeMillis, bearingTimeMillis / 1000, key);
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("alert", "lwm does not raise for a long time.", 1, 0);
                    removeServer(key);
                }
            } else {
                lastLWMHashMap.put(key, new BaseWatchActivity.LastLWM(currentLWM, currentProgress, currentTimeMillis));
                loggerP.info("go ahead ({}): lwm {} progress {}", key, currentLWM, currentProgress);
            }
            if (server.getStatus() == SystemStatus.STOPPED) {
                logger.info("server status is stopped, going to remove server ({})", key);
                removeServer(key);
            }
        } catch (Throwable t) {
            logger.error("patrol water mark error for: {}", key, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "watch");
        }
    }

    private void removeServer(String key) throws Exception {
        long startTime = System.currentTimeMillis();
        serverContainer.removeServer(key, true);
        serverContainer.registerServer(key);
        lastLWMHashMap.remove(key);
        logger.info("watch activity remove serve({}) cost time: {}ms", key, System.currentTimeMillis() - startTime);
    }
}
