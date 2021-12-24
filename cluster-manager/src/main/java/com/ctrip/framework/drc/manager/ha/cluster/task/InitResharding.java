package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterServers;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.xpipe.zk.ZkClient;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class InitResharding extends AbstractDirectMoveSharding{

    private Set<Integer> emptySlots;

    public InitResharding(SlotManager slotManager, Set<Integer> emptySlots, ClusterServers<?> servers, ZkClient zkClient) {
        super(slotManager, servers, zkClient);
        this.emptySlots = emptySlots;
    }

    @Override
    protected ClusterServer getDeadServer() {
        return null;
    }

    @Override
    protected List<Integer> getSlotsToArrange() {
        return new LinkedList<>(emptySlots);
    }

    @Override
    protected void doReset(){

    }
}
