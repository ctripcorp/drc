package com.ctrip.framework.drc.messenger.container.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;

import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.MESSENGER_REGISTER_PATH;

/**
 * @Author Slight
 * Dec 02, 2019
 */
public class MessengerZookeeperConfig extends AbstractZookeeperConfig {

    @Override
    protected String getPath() {
        return MESSENGER_REGISTER_PATH;
    } //TODO srx change with cm and zk
}
