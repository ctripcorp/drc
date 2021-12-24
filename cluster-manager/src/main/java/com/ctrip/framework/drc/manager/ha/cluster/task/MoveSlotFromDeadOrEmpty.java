package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotInfo;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class MoveSlotFromDeadOrEmpty extends AbstractSlotMoveTask {

    public MoveSlotFromDeadOrEmpty(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
        super(slot, from, to, zkClient);
    }

    public MoveSlotFromDeadOrEmpty(Integer slot, ClusterServer to, ZkClient zkClient) {
        super(slot, null, to, zkClient);
    }

    @Override
    protected void doExecute() throws Exception {

        logger.info("[doExecute]{},{}->{}", slot, from, to);
        setSlotInfo(new SlotInfo(getTo().getServerId()));
        to.addSlot(slot);
        future().setSuccess(null);
    }

    @Override
    protected void doReset(){

    }

}
