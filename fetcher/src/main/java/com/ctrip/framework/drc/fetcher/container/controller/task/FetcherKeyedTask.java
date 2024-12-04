package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;
import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName ApplierKeyedTask
 * @Author haodongPan
 * @Date 2024/1/2 17:31
 * @Version: $
 */
public class FetcherKeyedTask extends AbstractCommand {
    
    protected String registryKey;

    protected FetcherConfigDto applierConfig;

    protected FetcherServerContainer serverContainer;
    
    protected Logger logger = LoggerFactory.getLogger(getClass());
    
    public FetcherKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer) {
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
        return this.getClass().getSimpleName() + "-" +  registryKey;
    }
}
