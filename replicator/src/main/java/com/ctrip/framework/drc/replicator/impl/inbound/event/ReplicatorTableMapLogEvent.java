package com.ctrip.framework.drc.replicator.impl.inbound.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

/**
 * @Author limingdong
 * @create 2020/6/23
 */
public class ReplicatorTableMapLogEvent extends TableMapLogEvent {

    @Override
    protected boolean decodeColumn() {
        return false;
    }
}
