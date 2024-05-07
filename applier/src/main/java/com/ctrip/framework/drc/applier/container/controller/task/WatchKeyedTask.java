package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.activity.monitor.WatchActivity;
import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.server.ApplierServer;
import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.fetcher.system.SystemStatus;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName WatchKeyedTask
 * @Author haodongPan
 * @Date 2024/5/6 16:11
 * @Version: $
 */
public class WatchKeyedTask extends ApplierKeyedTask {

    private final Logger loggerP = LoggerFactory.getLogger("PROGRESS");

    private ConcurrentHashMap<String, WatchActivity.LastLWM> lastLWMHashMap;
    
    public WatchKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer,
            ConcurrentHashMap<String, WatchActivity.LastLWM> lastLWMHashMap) {
        super(registryKey, applierConfig, serverContainer);
        this.lastLWMHashMap = lastLWMHashMap;
    }


    @Override
    public void doExecute() throws Throwable {
        try {
            if (!serverContainer.containServer(registryKey)) {
                logger.info("[watch] applier instance({}) already remove by last Task,no need patrol", registryKey);
                future().setSuccess();
                return;
            }
            
            logger.info("[watch] applier instance({}) ", registryKey);
            patrol(registryKey, serverContainer.getServer(registryKey));
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[watch] applier instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "watch");
            future().setFailure(t);
        }
    }

    // catch throwable to avoid retry, wait next round
    protected void patrol(String key, ApplierServer server) {
        try {
            long currentLWM = server.getLWM();
            long currentProgress = server.getProgress();
            long bearingTimeMillis = ApplierDynamicConfig.getInstance().getLwmToleranceTime();
            if (currentLWM == 0)
                bearingTimeMillis = ApplierDynamicConfig.getInstance().getFirstLwmToleranceTime();
            long currentTimeMillis = System.currentTimeMillis();
            WatchActivity.LastLWM lastLWM = lastLWMHashMap.computeIfAbsent(key, k -> new WatchActivity.LastLWM(currentLWM, currentProgress, currentTimeMillis));
            if (lastLWM.lwm == currentLWM && lastLWM.progress == currentProgress) {
                if (currentTimeMillis - lastLWM.lastTimeMillis > bearingTimeMillis) {
                    logger.info("lwm does not raise since {}ms with bearing time {}s, going to remove server ({})", lastLWM.lastTimeMillis, bearingTimeMillis / 1000, key);
                    DefaultEventMonitorHolder.getInstance().logBatchEvent("alert", "lwm does not raise for a long time.", 1, 0);
                    removeServer(key);
                }
            } else {
                lastLWMHashMap.put(key, new WatchActivity.LastLWM(currentLWM, currentProgress, currentTimeMillis));
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
