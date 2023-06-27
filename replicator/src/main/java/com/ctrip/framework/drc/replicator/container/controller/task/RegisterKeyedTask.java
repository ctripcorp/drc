package com.ctrip.framework.drc.replicator.container.controller.task;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.container.ServerContainer;

/**
 * Created by jixinwang on 2023/6/25
 */
public class RegisterKeyedTask extends AbstractKeyedTask {

    public RegisterKeyedTask(String registryKey, ReplicatorConfig replicatorConfig, ServerContainer<ReplicatorConfig, ApiResult> serverContainer) {
        super(registryKey, replicatorConfig, serverContainer);
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Register] replicator instance for {} with {}", registryKey, replicatorConfig);
            serverContainer.register(registryKey, replicatorConfig.getApplierPort());
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[Register] replicator instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.instance.error", "register");
            future().setFailure(t);
        }
    }
}
