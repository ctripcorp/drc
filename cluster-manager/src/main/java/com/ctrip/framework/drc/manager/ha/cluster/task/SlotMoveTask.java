package com.ctrip.framework.drc.manager.ha.cluster.task;

import com.ctrip.framework.drc.manager.ha.cluster.ClusterServer;
import com.ctrip.xpipe.api.command.Command;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public interface SlotMoveTask extends Command<Void> {

    int getSlot();

    ClusterServer getFrom();

    ClusterServer getTo();



}
