package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.ShardingException;
import com.ctrip.framework.drc.manager.ha.cluster.SlotInfo;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public abstract class AbstractSlotMoveTask extends AbstractCommand<Void> implements SlotMoveTask{

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected int taskMaxWaitMilli = 10000;

    protected final Integer slot;

    protected final ClusterServer from, to;

    protected ZkClient zkClient;

    public AbstractSlotMoveTask(Integer slot, ClusterServer from, ClusterServer to, ZkClient zkClient) {
        this.slot = slot;
        this.from = from;
        this.to = to;
        this.zkClient = zkClient;
    }

    @Override
    public ClusterServer getFrom() {
        return from;
    }

    protected void setSlotInfo(SlotInfo slotInfo) throws ShardingException {

        CuratorFramework client = zkClient.get();

        String path = getSlotZkPath();
        try {
            client.createContainers(path);
            client.setData().forPath(path, slotInfo.encode());
        } catch (Exception e) {
            throw new ShardingException(String.format("path:%s, slotInfo:%s", path, slotInfo), e);
        }
    }


    @Override
    public int getSlot() {
        return slot;
    }

    @Override
    public ClusterServer getTo() {
        return to;
    }

    protected String getSlotZkPath() {
        return String.format("%s/%d", ClusterZkConfig.getClusterManagerSlotsPath(), slot);
    }

    @Override
    public String toString() {
        return String.format("(%s)slot:%d, %s->%s", getClass().getSimpleName(), slot, from == null? "null" : from.getServerId(), to == null ? null: to.getServerId());
    }


    @Override
    public String getName() {
        return getClass().getSimpleName();
    }
}
