package com.ctrip.framework.drc.console.ha.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperConfig;

/**
 * @ClassName ConsoleZkCofig
 * @Author haodongPan
 * @Date 2022/6/24 16:30
 * @Version: $
 */
public class ConsoleZkConfig extends AbstractZookeeperConfig {
    
    @Override
    protected String getPath() {
        return null;
    }
}
