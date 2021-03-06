package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.*;
import com.ctrip.xpipe.zk.ZkClient;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ContinueResharding extends AbstractResharding{

    private Map<Integer, SlotInfo> slotInfos;
    private RemoteClusterServerFactory<? extends ClusterServer> remoteClusterServerFactory;

    public ContinueResharding(SlotManager slotManager, Map<Integer, SlotInfo> slotInfos, ClusterServers<?> servers, RemoteClusterServerFactory<?> remoteClusterServerFactory, ZkClient zkClient) {
        super(slotManager, servers, zkClient);
        this.slotInfos = slotInfos;
        this.remoteClusterServerFactory = remoteClusterServerFactory;

    }

    @Override
    protected void doShardingTask() throws ShardingException {

        logger.info("[doShardingTask]{}", slotInfos);

        for(Map.Entry<Integer, SlotInfo> entry : slotInfos.entrySet()){

            Integer slot = entry.getKey();
            SlotInfo slotInfo = entry.getValue();

            if(slotInfo.getSlotState() != SLOT_STATE.MOVING){
                logger.warn("[doExecute][state not moving]{}", slotInfo);
                continue;
            }

            ClusterServer from = servers.getClusterServer(slotInfo.getServerId());
            ClusterServer to = servers.getClusterServer(slotInfo.getToServerId());
            if(to != null){
                if(from != null){
                    executeTask(new MoveSlotFromLiving(slot, from, to, zkClient));
                }else{
                    executeTask(new MoveSlotFromDeadOrEmpty(slot, remoteClusterServerFactory.createClusterServer(slotInfo.getServerId(), null), to, zkClient));
                }
            }else{
                executeTask(new RollbackMovingTask(slot, from, null, zkClient));
            }
        }
    }

    @Override
    protected void doReset(){

    }



}
