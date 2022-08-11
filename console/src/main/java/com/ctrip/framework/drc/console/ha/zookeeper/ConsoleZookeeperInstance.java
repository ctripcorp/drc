package com.ctrip.framework.drc.console.ha.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperInstance;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName ConsoleZookeeperInstance
 * @Author haodongPan
 * @Date 2022/6/24 16:27
 * @Version: $
 */
@Configuration
public class ConsoleZookeeperInstance extends AbstractZookeeperInstance {
    
    @Bean
    public DrcZkConfig getZkConfig() {
        return new ConsoleZkConfig();
    }
}
