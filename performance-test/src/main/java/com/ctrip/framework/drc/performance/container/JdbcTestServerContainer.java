package com.ctrip.framework.drc.performance.container;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.server.ApplierServerInCluster;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.performance.impl.server.JdbcTestServer;

/**
 * Created by jixinwang on 2021/9/9
 */
public class JdbcTestServerContainer extends ApplierServerContainer {

    @Override
    protected ApplierServerInCluster getApplierServer(ApplierConfigDto config) throws Exception {
        return new JdbcTestServer(config);
    }
}
