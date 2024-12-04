package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;

/**
 * Created by shiruixin
 * 2024/11/20 15:24
 */
public class BaseRegisterKeyedTask extends FetcherKeyedTask  {

    public BaseRegisterKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() throws Throwable {
        serverContainer.registerServer(registryKey);
    }
}
