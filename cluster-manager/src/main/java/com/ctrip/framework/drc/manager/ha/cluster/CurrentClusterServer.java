package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface CurrentClusterServer extends ClusterServer, Lifecycle {

    int ORDER = SlotManager.ORDER + 1;

    Set<Integer> slots();

    boolean isLeader();

    boolean hasKey(Object key);

}
