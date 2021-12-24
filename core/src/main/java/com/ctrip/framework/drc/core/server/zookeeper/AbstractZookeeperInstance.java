package com.ctrip.framework.drc.core.server.zookeeper;

import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import com.ctrip.xpipe.zk.impl.SpringZkClient;
import org.springframework.context.annotation.Bean;

/**
 * Created by mingdongli
 * 2019/11/3 上午9:34.
 */
public class AbstractZookeeperInstance extends AbstractProfile {

    private int zkSessionTimeoutMillis = 10000;

    @Bean
    public ZkClient getZkClient(DrcZkConfig drcZkConfig) {
        return getZkClient(drcZkConfig.getZkNamespace(), drcZkConfig.getZkConnectionString());
    }

    @Override
    protected ZkClient getZkClient(String zkNameSpace, String zkAddress) {

        DefaultZkConfig zkConfig = new DefaultZkConfig();
        zkConfig.setZkNameSpace(zkNameSpace);
        zkConfig.setZkSessionTimeoutMillis(zkSessionTimeoutMillis);

        SpringZkClient springZkClient = new SpringZkClient(zkConfig, zkAddress);
        return springZkClient;
    }

}
