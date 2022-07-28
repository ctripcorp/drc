package com.ctrip.framework.drc.monitor.module;

import com.ctrip.framework.drc.core.config.TestConfig;

/**
 * Created by mingdongli
 * 2019/10/15 上午1:35.
 */
public interface RAModule extends DrcModule {

    void startRAModule(TestConfig srcConfig, TestConfig destConfig);
}
