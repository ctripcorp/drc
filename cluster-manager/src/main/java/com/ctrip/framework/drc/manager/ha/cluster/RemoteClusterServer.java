package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface RemoteClusterServer extends ClusterServer, Lifecycle {

    String getCurrentServerId();
}
