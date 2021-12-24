package com.ctrip.framework.drc.manager.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.MANAGER_REGISTER_PATH;

/**
 * Created by mingdongli
 * 2019/11/3 上午9:38.
 */
public class DefaultManagerConfig extends AbstractZookeeperConfig {

    @Override
    protected String getPath() {
        return MANAGER_REGISTER_PATH;
    }
}
