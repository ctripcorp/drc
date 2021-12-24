package com.ctrip.framework.drc.manager.zookeeper;

import com.ctrip.framework.drc.core.server.zookeeper.AbstractZookeeperInstance;
import com.ctrip.framework.drc.core.server.zookeeper.DrcZkConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Created by mingdongli
 * 2019/11/2 下午5:08.
 */
@Configuration
public class ManagerZookeeperInstance extends AbstractZookeeperInstance {

    @Bean
    @Primary
    public DrcZkConfig getZkConfig(){
        return new DefaultManagerConfig();
    }

}
