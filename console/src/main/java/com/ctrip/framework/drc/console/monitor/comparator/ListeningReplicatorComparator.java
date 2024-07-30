package com.ctrip.framework.drc.console.monitor.comparator;

import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.google.common.collect.Sets;
import org.unidal.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jixinwang on 2021/8/3
 */
public class ListeningReplicatorComparator extends AbstractMetaComparator<String, ReplicatorWrapperChange> {

    private Map<String, ReplicatorWrapper> current, future;

    public ListeningReplicatorComparator(Map<String, ReplicatorWrapper> current, Map<String, ReplicatorWrapper> future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
        Triple<Set<String>, Set<String>, Set<String>> result = getDiff(Sets.newHashSet(current.keySet()),
                Sets.newHashSet(future.keySet()));

        added = result.getFirst();
        removed = result.getLast();
        Set<String> intersectionClusterIds = result.getMiddle();

        for(String dbClusterId : intersectionClusterIds) {
            ReplicatorWrapper currentReplicatorWrapper = current.get(dbClusterId);
            ReplicatorWrapper futureReplicatorWrapper = future.get(dbClusterId);
            if(currentReplicatorWrapper == null || futureReplicatorWrapper == null) {
                modified.add(new ReplicatorWrapperComparator(dbClusterId));
                continue;
            }
            if(currentReplicatorWrapper.equals(futureReplicatorWrapper)) {
                Dc currentDc = new Dc();
                addDcRoutes(currentDc, currentReplicatorWrapper.getRoutes());
                Dc futureDc = new Dc();
                addDcRoutes(futureDc, futureReplicatorWrapper.getRoutes());
                DcRouteComparator dcRouteComparator = new DcRouteComparator(currentDc, futureDc, Route.TAG_CONSOLE);
                dcRouteComparator.compare();
                Set<Route> removedRoutes = dcRouteComparator.getRemoved();
                Set<MetaComparator> modifiedRoutesComparator = dcRouteComparator.getMofified();
                Set<Route> added = dcRouteComparator.getAdded();

                if(!removedRoutes.isEmpty() || !modifiedRoutesComparator.isEmpty() || !added.isEmpty()) {
                    modified.add(new ReplicatorWrapperComparator(dbClusterId));
                }
            } else {
                modified.add(new ReplicatorWrapperComparator(dbClusterId));
            }
        }
    }

    private void addDcRoutes(Dc dc, List<Route> routes) {
        if(routes != null) {
            for(Route route : routes) {
                dc.addRoute(route);
            }
        }
    }

    @Override
    public String idDesc() {
        return null;
    }
}
