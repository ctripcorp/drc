package com.ctrip.framework.drc.fetcher.container.controller.task;

import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;

/**
 * Created by shiruixin
 * 2024/11/20 15:21
 */
public class BaseDeleteKeyedTask extends FetcherKeyedTask {

    protected boolean isDelete = true; // default is true

    public BaseDeleteKeyedTask(String registryKey, FetcherConfigDto applierConfig, FetcherServerContainer serverContainer, boolean isDelete) {
        super(registryKey, applierConfig, serverContainer);
        this.isDelete = isDelete;
    }

    @Override
    protected void doExecute() throws Throwable {
        serverContainer.removeServer(registryKey, isDelete);
    }
}
