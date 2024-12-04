package com.ctrip.framework.drc.messenger.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseDeleteKeyedTask;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;

/**
 * @ClassName DeleteKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:49
 * @Version: $
 */
public class DeleteKeyedTask extends BaseDeleteKeyedTask {
    
    public DeleteKeyedTask(String registryKey, MessengerConfigDto applierConfig, MqServerContainer serverContainer, boolean isDelete) {
        super(registryKey, applierConfig, serverContainer, isDelete);
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Remove] messenger instance with {},isDelete {}", registryKey,isDelete);
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[Remove] messenger instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.instance.error", "remove");
            future().setFailure(t);
        }
    }


}
