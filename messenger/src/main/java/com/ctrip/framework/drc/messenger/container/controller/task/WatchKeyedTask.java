package com.ctrip.framework.drc.messenger.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseWatchKeyedTask;
import com.ctrip.framework.drc.messenger.activity.monitor.WatchActivity;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName WatchKeyedTask
 * @Author haodongPan
 * @Date 2024/5/6 16:11
 * @Version: $
 */
public class WatchKeyedTask extends BaseWatchKeyedTask {
    
    public WatchKeyedTask(String registryKey, MessengerConfigDto applierConfig, MqServerContainer serverContainer,
                          ConcurrentHashMap<String, WatchActivity.LastLWM> lastLWMHashMap) {
        super(registryKey, applierConfig, serverContainer, lastLWMHashMap);
    }


    @Override
    public void doExecute() throws Throwable {
        try {
            if (!serverContainer.containServer(registryKey)) {
                logger.info("[watch] messenger instance({}) already remove by last Task,no need patrol", registryKey);
                future().setSuccess();
                return;
            }
            
            logger.info("[watch] messenger instance({}) ", registryKey);
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[watch] messenger instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.instance.error", "watch");
            future().setFailure(t);
        }
    }
}
