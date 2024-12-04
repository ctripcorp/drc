package com.ctrip.framework.drc.messenger.container.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperInstance;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author Slight
 * Dec 02, 2019
 */

@Configuration
public class MessengerZookeeperInstance extends AbstractZookeeperInstance {

    @Bean
    public DrcZkConfig getZkConfig(){
        return new MessengerZookeeperConfig();
    }

    @Bean
    public LeaderElectorManager geElectorManager(ZkClient zkClient){
        return new DefaultLeaderElectorManager(zkClient);
    }
}
