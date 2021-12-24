package com.ctrip.framework.drc.core.meta.comparator;

import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.Set;

/**
 * @Author: hbshen
 * @Date: 2021/4/14
 */
public class DcRouteComparator extends AbstractMetaComparator<Route, RouteChange> {

    private Dc current, future;

    private String routeTagFilter;

    public DcRouteComparator(Dc current, Dc future, String routeTagFilter) {
        this.current = current;
        this.future = future;
        this.routeTagFilter = routeTagFilter;
    }

    public DcRouteComparator(Dc current, Dc future) {
        this(current, future, Route.TAG_META);
    }

    @Override
    public void compare() {
        Triple<Set<Route>, Set<Route>, Set<Route>> result = getDiff(Sets.newHashSet(current.getRoutes()),
                Sets.newHashSet(future.getRoutes()));

        Set<Route> intersectionRoutes = filterTag(result.getMiddle());
        added = filterTag(result.getFirst());
        removed = filterTag(result.getLast());

        for(Route route : intersectionRoutes) {
            Route currentRoute = getRoute(current, route.getId());
            Route futureRoute = getRoute(future, route.getId());
            if(currentRoute == null || futureRoute == null) {
                modified.add(new RouteComparator(currentRoute, futureRoute));
                continue;
            }
            if(!currentRoute.getRouteInfo().equalsIgnoreCase(futureRoute.getRouteInfo())) {
                modified.add(new RouteComparator(currentRoute, futureRoute));
            }
        }
    }

    private Route getRoute(Dc dc, Integer id) {
        for(Route route : dc.getRoutes()) {
            if(route.getId().equals(id)) {
                return route;
            }
        }
        return null;
    }

    private Set<Route> filterTag(Set<Route> routes) {
        if(routes == null || routes.isEmpty()) {
            return routes;
        }
        return Sets.newHashSet(Sets.filter(routes, route -> {
            assert route != null;
            return routeTagFilter.equalsIgnoreCase(route.getTag());
        }));
    }

    @Override
    public String idDesc() {
        return current.getId();
    }
}
