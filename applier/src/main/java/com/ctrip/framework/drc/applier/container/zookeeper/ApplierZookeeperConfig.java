package com.ctrip.framework.drc.applier.container.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.APPLIER_REGISTER_PATH;

/**
 * @Author Slight
 * Dec 02, 2019
 */
public class ApplierZookeeperConfig extends AbstractZookeeperConfig {

    @Override
    protected String getPath() {
        return APPLIER_REGISTER_PATH;
    }
}
