package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.cluster.AbstractLeaderElector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
@Component
public class ClusterManagerLeaderElector extends AbstractLeaderElector implements TopElement {

    @Autowired
    private ClusterManagerConfig config;

    @Override
    protected String getServerId() {
        return config.getClusterServerId();
    }

    @Override
    protected String getLeaderElectPath() {
        return ClusterZkConfig.getClusterManagerLeaderElectPath();
    }
}
