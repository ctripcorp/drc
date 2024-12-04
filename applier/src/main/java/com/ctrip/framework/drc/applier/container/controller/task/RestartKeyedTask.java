package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseRestartKeyedTask;

public class RestartKeyedTask extends BaseRestartKeyedTask {

    public RestartKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Restart] applier instance for {} with {}", registryKey, applierConfig);
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("Restart] error in applier instance for {} with {}", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "restart");
            future().setFailure(t);
        }
    }
}
