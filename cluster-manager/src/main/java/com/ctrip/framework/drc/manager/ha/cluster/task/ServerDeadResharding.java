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
public class ServerDeadResharding extends AbstractDirectMoveSharding{

    private ClusterServer deadServer;

    public ServerDeadResharding(SlotManager slotManager, ClusterServer deadServer, ClusterServers<? extends ClusterServer> servers, ZkClient zkClient) {
        super(slotManager, servers, zkClient);
        this.deadServer = deadServer;
    }



    @Override
    protected void doReset(){
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<Integer> getSlotsToArrange() {

        Set<Integer> slots = slotManager.getSlotsByServerId(deadServer.getServerId());
        logger.info("[doRun][serverDead, resharding]{}, {}", deadServer, slots);

        List<Integer> result = new LinkedList<>();
        if(slots != null){
            result.addAll(slots);
        }
        return result;
    }

    @Override
    protected List<ClusterServer> allAliveServers() {

        List<ClusterServer> result = super.allAliveServers();
        result.remove(deadServer);
        return result;
    }

    @Override
    protected ClusterServer getDeadServer() {
        return deadServer;
    }


}
