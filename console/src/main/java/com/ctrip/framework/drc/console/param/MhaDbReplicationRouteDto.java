package com.ctrip.framework.drc.console.param;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/4/7 15:13
 */
public class MhaDbReplicationRouteDto {

    private List<Long> mhaDbReplicationIds;
    private List<Long> mhaIds;
    private Long routeId;

    public List<Long> getMhaIds() {
        return mhaIds;
    }

    public void setMhaIds(List<Long> mhaIds) {
        this.mhaIds = mhaIds;
    }

    public List<Long> getMhaDbReplicationIds() {
        return mhaDbReplicationIds;
    }

    public void setMhaDbReplicationIds(List<Long> mhaDbReplicationIds) {
        this.mhaDbReplicationIds = mhaDbReplicationIds;
    }

    public Long getRouteId() {
        return routeId;
    }

    public void setRouteId(Long routeId) {
        this.routeId = routeId;
    }
}
