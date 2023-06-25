package com.ctrip.framework.drc.replicator.container.controller.task;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.xpipe.command.AbstractCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jixinwang on 2023/6/25
 */
public abstract class AbstractKeyedTask extends AbstractCommand {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected String registryKey;

    protected ReplicatorConfig replicatorConfig;

    protected ServerContainer<ReplicatorConfig, ApiResult> serverContainer;

    public AbstractKeyedTask(String registryKey, ReplicatorConfig replicatorConfig, ServerContainer<ReplicatorConfig, ApiResult> serverContainer) {
        this.registryKey = registryKey;
        this.replicatorConfig = replicatorConfig;
        this.serverContainer = serverContainer;
    }

    @Override
    protected void doExecute() {
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return registryKey;
    }
}
