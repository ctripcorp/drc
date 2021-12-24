package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Map;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface SlotManager extends Lifecycle {

    int ORDER = 0;

    int TOTAL_SLOTS = Integer.parseInt(System.getProperty("TOTAL_SLOTS", "256"));//change only for unit test

    SlotInfo getSlotInfo(int slotId);

    String getSlotServerId(int slotId);

    Set<Integer> getSlotsByServerId(String serverId);

    Set<Integer> getSlotsByServerId(String serverId, boolean includeMoving);

    int getSlotsSizeByServerId(String serverId);

    void refresh() throws ClusterException;

    void refresh(int ...slotIds) throws ClusterException;

    void move(int slotId, String fromServer, String toServer);

    Set<Integer>  allSlots();

    Set<String> allServers();

    Map<Integer, SlotInfo> allMoveingSlots();

    int getSlotIdByKey(Object key);

    SlotInfo getSlotInfoByKey(Object key);

    String getServerIdByKey(Object key);

    Map<Integer, SlotInfo> allSlotsInfo();

}
