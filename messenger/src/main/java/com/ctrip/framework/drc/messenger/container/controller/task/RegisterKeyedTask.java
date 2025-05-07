package com.ctrip.framework.drc.messenger.container.controller.task;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.fetcher.container.controller.task.BaseRegisterKeyedTask;
import com.ctrip.framework.drc.messenger.container.MqServerContainer;

/**
 * @ClassName ApplierRegisterKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:45
 * @Version: $
 */
public class RegisterKeyedTask extends BaseRegisterKeyedTask {

    public RegisterKeyedTask(String registryKey, MessengerConfigDto applierConfig, MqServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() {
        try {
            logger.info("[Register] messenger instance for {} with {}", registryKey, applierConfig);
            super.doExecute();
            future().setSuccess();
        } catch (Throwable t) {
            logger.error("Register] error in messenger instance for {} with {}", registryKey, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.messenger.instance.error", "register");
            future().setFailure(t);
        }
    }
}
