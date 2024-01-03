package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;

/**
 * @ClassName DeleteKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:49
 * @Version: $
 */
public class DeleteKeyedTask extends ApplierKeyedTask {
    
    private boolean isDelete = true; // default is true
    
    public DeleteKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer,boolean isDelete) {
        super(registryKey, applierConfig, serverContainer);
        this.isDelete = isDelete;
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Remove] applier instance with {},isDelete {}", registryKey,isDelete);
            serverContainer.removeServer(registryKey, isDelete);
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("[Remove] applier instance({}) error", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "remove");
            future().setFailure(t);
        }
    }


}
