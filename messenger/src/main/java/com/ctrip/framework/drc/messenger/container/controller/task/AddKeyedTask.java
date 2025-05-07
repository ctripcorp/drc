package com.ctrip.framework.drc.messenger.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseAddKeyedTask;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;

/**
 * @ClassName ApplierAddKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:45
 * @Version: $
 */
public class AddKeyedTask extends BaseAddKeyedTask {

    public AddKeyedTask(String registryKey, MessengerConfigDto applierConfig, MqServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }
    
    @Override
    public void doExecute() throws Throwable {
       try {
           logger.info("[Start] messenger instance({}) with {}", registryKey, applierConfig);
           super.doExecute();
           future().setSuccess();
       } catch (Throwable t) {
           logger.error("[Start] messenger instance({}) error", registryKey, t);
           DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.instance.error", "start");
           future().setFailure(t);
       }
    }
}
