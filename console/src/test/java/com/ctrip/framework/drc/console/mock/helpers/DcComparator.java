package com.ctrip.framework.drc.console.mock.helpers;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import org.unidal.tuple.Triple;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class DcComparator extends AbstractMetaComparator<DbCluster, DcChange> {

    private Dc current, future;

    public static DcComparator buildComparator(Dc current, Dc future){

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();
        return dcMetaComparator;
    }

    public static DcComparator buildClusterRemoved(DbCluster clusterMeta){
        DcComparator dcMetaComparator = new DcComparator();
        dcMetaComparator.removed.add(clusterMeta);
        return dcMetaComparator;
    }

    public static DcComparator buildClusterChanged(DbCluster current, DbCluster future){

        DcComparator dcMetaComparator = new DcComparator();
        if(current == null){
            dcMetaComparator.added.add(future);
            return dcMetaComparator;
        }

        ClusterComparator clusterComparator = new ClusterComparator(current, future);
        clusterComparator.compare();
        dcMetaComparator.modified.add(clusterComparator);
        return dcMetaComparator;
    }

    private DcComparator(){

    }

    public DcComparator(Dc current, Dc future){
        this.current = current;
        this.future = future;
    }

    public void compare(){

        Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getDbClusters().keySet(), future.getDbClusters().keySet());

        Set<String> addedClusterIds = result.getFirst();
        Set<String> intersectionClusterIds = result.getMiddle();
        Set<String> deletedClusterIds = result.getLast();

        for(String clusterId : addedClusterIds){
            added.add(future.findDbCluster(clusterId));
        }

        for(String clusterId : deletedClusterIds){
            removed.add(current.findDbCluster(clusterId));
        }

        for(String clusterId : intersectionClusterIds){
            DbCluster currentMeta = current.findDbCluster(clusterId);
            DbCluster futureMeta = future.findDbCluster(clusterId);
            if(!reflectionEquals(currentMeta, futureMeta)){
                ClusterComparator clusterComparator = new ClusterComparator(currentMeta, futureMeta);
                clusterComparator.compare();
                modified.add(clusterComparator);
            }
        }
    }

    @Override
    public String idDesc() {

        if(current != null){
            return current.getId();
        }
        if(future != null){
            return future.getId();
        }
        return null;
    }
}
