package com.ctrip.framework.drc.applier.container.controller.task;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName ApplierKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:31
 * @Version: $
 */
public class ApplierKeyedTask extends AbstractCommand {
    
    protected String registryKey;

    protected ApplierConfigDto applierConfig;

    protected ApplierServerContainer serverContainer;
    
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public ApplierKeyedTask(String registryKey, ApplierConfigDto applierConfig, ApplierServerContainer serverContainer) {
        this.registryKey = registryKey;
        this.applierConfig = applierConfig;
        this.serverContainer = serverContainer;
    }

    @Override
    protected void doExecute() throws Throwable {
        
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return null;
    }
}
