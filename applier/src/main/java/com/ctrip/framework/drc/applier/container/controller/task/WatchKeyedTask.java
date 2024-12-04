package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.fetcher.activity.monitor.BaseWatchActivity;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseWatchKeyedTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName WatchKeyedTask
 * @Author haodongPan
 * @Date 2024/5/6 16:11
 * @Version: $
 */
public class WatchKeyedTask extends BaseWatchKeyedTask {
    
    public WatchKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer,
            ConcurrentHashMap<String, BaseWatchActivity.LastLWM> lastLWMHashMap) {
        super(registryKey, applierConfig, serverContainer,lastLWMHashMap);
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
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[watch] applier instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "watch");
            future().setFailure(t);
        }
    }
}
