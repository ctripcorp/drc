package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface ClusterServer {

    String getServerId();

    ClusterServerInfo getClusterInfo();

    /**
     * reresh slotmanager
     * @return
     */
    CommandFuture<Void> addSlot(int slot);

    CommandFuture<Void> deleteSlot(int slot);

    /**
     * notify server to export slot
     * @param slotId
     */
    CommandFuture<Void> exportSlot(int slotId);

    /**
     * notify server to import slot
     * @param slotId
     */
    CommandFuture<Void> importSlot(int slotId);

    void notifySlotChange(int slotId);

}
