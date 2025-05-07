package com.ctrip.framework.drc.console.vo.request;

import java.io.Serializable;
import java.util.List;


public class MhaDbDelayQueryDto  implements Serializable {
    private List<Long> replicationIds;

    public List<Long> getReplicationIds() {
        return replicationIds;
    }

    public void setReplicationIds(List<Long> replicationIds) {
        this.replicationIds = replicationIds;
    }
}
