package com.ctrip.framework.drc.performance.container;

import com.ctrip.framework.drc.applier.container.ApplierServerContainer;
import com.ctrip.framework.drc.applier.server.ApplierServerInCluster;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.performance.impl.server.ApplierTestServerInCluster;
import com.ctrip.framework.drc.performance.impl.server.TransactionTableApplierTestServerInCluster;

/**
 * Created by jixinwang on 2021/9/14
 */
public class ApplierTestServerContainer extends ApplierServerContainer {

    @Override
    protected ApplierServerInCluster getApplierServer(ApplierConfigDto config) throws Exception {
        ApplyMode applyMode = ApplyMode.getApplyMode(config.getApplyMode());
        logger.info("start to add applier servers for {}, apply mode is: {}", config.getRegistryKey(), applyMode.getName());
        switch (applyMode) {
            case transaction_table:
                return new TransactionTableApplierTestServerInCluster(config);
            default:
                return new ApplierTestServerInCluster(config);
        }
    }
}
