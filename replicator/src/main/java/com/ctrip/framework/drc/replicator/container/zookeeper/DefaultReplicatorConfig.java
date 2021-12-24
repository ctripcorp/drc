package com.ctrip.framework.drc.replicator.container.zookeeper;


import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.REPLICATOR_REGISTER_PATH;

/**
 * Created by mingdongli
 * 2019/10/30 下午11:46.
 */
public class DefaultReplicatorConfig extends AbstractZookeeperConfig {

    @Override
    protected String getPath() {
        return REPLICATOR_REGISTER_PATH;
    }
}
