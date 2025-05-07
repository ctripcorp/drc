package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;

/**
 * Created by shiruixin
 * 2024/11/20 15:27
 */
public class BaseRestartKeyedTask extends FetcherKeyedTask {

    public BaseRestartKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() throws Throwable{
        if (serverContainer.getServer(registryKey) == null) {
            logger.error("[Restart] {} fail, server not exist", registryKey);
            return;
        }
        serverContainer.removeServer(registryKey, true);
        serverContainer.registerServer(registryKey);
    }
}
