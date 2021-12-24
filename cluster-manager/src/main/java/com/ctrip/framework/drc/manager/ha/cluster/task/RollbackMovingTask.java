package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class RollbackMovingTask extends AbstractSlotMoveTask{

    public RollbackMovingTask(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
        super(slot, from, to, zkClient);
    }

    @Override
    protected void doExecute() throws Exception {

        setSlotInfo(new SlotInfo(from.getServerId()));

        ClusterServer from = getFrom();
        ClusterServer to = getTo();
        if( from != null ){
            from.addSlot(slot);
        }
        if( to != null ){
            to.deleteSlot(slot);
        }
        future().setSuccess();
    }

    @Override
    protected void doReset() {

    }
}
