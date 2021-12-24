package com.ctrip.framework.drc.replicator.container.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperInstance;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by mingdongli
 * 2019/10/31 上午12:00.
 */
@Configuration
public class ReplicatorZookeeperInstance extends AbstractZookeeperInstance {

    @Bean
    public DrcZkConfig getZkConfig(){
        return new DefaultReplicatorConfig();
    }

    @Bean
    public LeaderElectorManager geElectorManager(ZkClient zkClient){
        return new DefaultLeaderElectorManager(zkClient);
    }
}
