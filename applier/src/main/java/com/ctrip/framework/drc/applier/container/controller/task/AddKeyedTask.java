package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;

/**
 * @ClassName ApplierAddKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:45
 * @Version: $
 */
public class AddKeyedTask extends ApplierKeyedTask {

    public AddKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }
    
    @Override
    public void doExecute() throws Throwable {
       try {
           logger.info("[Start] applier instance({}) with {}", registryKey, applierConfig);
           serverContainer.addServer(applierConfig);
           future().setSuccess();
       } catch (Throwable t) {
           logger.error("[Start] applier instance({}) error", registryKey, t);
           DefaultEventMonitorHolder.getInstance().logEvent("DRC.applier.instance.error", "start");
           future().setFailure(t);
       }
    }
}
