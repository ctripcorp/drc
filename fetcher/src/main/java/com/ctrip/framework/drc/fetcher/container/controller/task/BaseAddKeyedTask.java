package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;

/**
 * Created by shiruixin
 * 2024/11/20 15:14
 */
public class BaseAddKeyedTask extends FetcherKeyedTask {

    public BaseAddKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer) {
        super(registryKey, applierConfig, serverContainer);
    }

    @Override
    protected void doExecute() throws Throwable {
        serverContainer.addServer(applierConfig);
    }
}
