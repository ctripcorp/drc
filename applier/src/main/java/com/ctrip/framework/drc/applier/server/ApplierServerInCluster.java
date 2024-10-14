package com.ctrip.framework.drc.applier.server;

import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;

/**
 * @Author Slight
 * Dec 01, 2019
 */
public class ApplierServerInCluster extends ApplierServer {

    public static final int DEFAULT_APPLY_COUNT = 10;
    public static final int TRANSACTION_TABLE_APPLY_COUNT = 100;

    public ApplierConfigDto config;

    public ApplierServerInCluster(ApplierConfigDto config) throws Exception {
        this.config = config;
        parseConfig(config);
        setConfig(config, ApplierConfigDto.class);
        setName(config.getRegistryKey());
        define();
    }

    @Override
    public void doDispose() throws Exception {
        super.doDispose();
    }

    private void parseConfig(ApplierConfigDto config) throws Exception {
        String properties = config.getProperties();
        Integer concurrency = DataMediaConfig.getConcurrency(properties);

        int applyConcurrency;
        switch (ApplyMode.getApplyMode(config.getApplyMode())) {
            case transaction_table:
                applyConcurrency = TRANSACTION_TABLE_APPLY_COUNT;
                break;
            case mq:
                applyConcurrency = ApplierDynamicConfig.getInstance().getMqApplyCount();
                break;
            default:
                applyConcurrency = DEFAULT_APPLY_COUNT;
        }

        if (concurrency != null && concurrency > 0 && concurrency <= 100) {
            applyConcurrency = concurrency;
        }
        config.setApplyConcurrency(applyConcurrency);
    }
}
