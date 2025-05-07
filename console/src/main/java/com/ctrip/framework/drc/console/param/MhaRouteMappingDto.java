package com.ctrip.framework.drc.console.param;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/4/15 14:29
 */
public class MhaRouteMappingDto {

    private List<Long> routeIds;
    private String mhaName;

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<Long> getRouteIds() {
        return routeIds;
    }

    public void setRouteIds(List<Long> routeIds) {
        this.routeIds = routeIds;
    }
}
