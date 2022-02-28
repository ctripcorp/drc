package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class ApplierServerInCluster extends ApplierServer {

    public ApplierConfigDto config;

    private ApplierServerContainer container;

    public ApplierServerInCluster(ApplierConfigDto config) throws Exception {
        setConfig(config, ApplierConfigDto.class);
        setName(config.getRegistryKey());
        define();
        this.config = config;
    }

    @Override
    public void doDispose() throws Exception {
        super.doDispose();
    }

    public ApplierServerContainer getContainer() {
        return container;
    }

    public void setContainer(ApplierServerContainer container) {
        this.container = container;
    }
}
