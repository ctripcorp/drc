package com.ctrip.framework.drc.messenger.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseRestartKeyedTask;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;

public class RestartKeyedTask extends BaseRestartKeyedTask {

    public RestartKeyedTask(String registryKey, MessengerConfigDto applierConfig, MqServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Restart] messenger instance for {} with {}", registryKey, applierConfig);
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("Restart] error in messenger instance for {} with {}", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.instance.error", "restart");
            future().setFailure(t);
        }
    }
}
