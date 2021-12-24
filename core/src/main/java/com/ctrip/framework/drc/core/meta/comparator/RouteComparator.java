package com.ctrip.framework.drc.core.meta.comparator;

import com.ctrip.framework.drc.core.entity.Route;

/**
 * @Author: hbshen
 * @Date: 2021/4/14
 */
public class RouteComparator extends AbstractMetaComparator<Route, RouteChange> {

    private Route current;

    private Route future;

    public RouteComparator(Route current, Route future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {

    }

    @Override
    public String idDesc() {
        return future.getRouteInfo();
    }

    public Route getCurrent() {
        return current;
    }

    public Route getFuture() {
        return future;
    }
}