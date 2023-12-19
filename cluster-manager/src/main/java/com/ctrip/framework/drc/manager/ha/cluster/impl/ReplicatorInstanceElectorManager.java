package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.Constants;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.container.ZookeeperValue;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.codec.JsonCodec;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
@Component
public class ReplicatorInstanceElectorManager extends AbstractInstanceElectorManager implements InstanceElectorManager, Observer, TopElement {

    @Override
    protected String getLeaderPath(String registryKey) {
        return ClusterZkConfig.getReplicatorLeaderLatchPath(registryKey);
    }

    @Override
    protected String getType() {
        return Constants.ENTITY_REPLICATOR;
    }

    @Override
    protected boolean watchIfNotWatched(String registryKey) {
        return currentMetaManager.watchReplicatorIfNotWatched(registryKey);
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();
        observerClusterLeader(clusterId);
    }

    protected void updateClusterLeader(String leaderLatchPath, List<ChildData> childrenData, String clusterId){

        List<String> childrenPaths = new LinkedList<>();
        childrenData.forEach(childData -> childrenPaths.add(childData.getPath()));

        logger.info("[updateShardLeader]{}, {}", clusterId, childrenPaths);

        List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, childrenPaths);

        List<Replicator> survivalReplicators = new ArrayList<>(childrenData.size());

        for(String path : sortedChildren){
            for(ChildData childData : childrenData){
                if(path.equals(childData.getPath())){
                    String data = new String(childData.getData());
                    ZookeeperValue zookeeperValue = JsonCodec.INSTANCE.decode(data, ZookeeperValue.class);
                    Replicator replicator = getReplicator(clusterId, zookeeperValue.getIp(), zookeeperValue.getPort());
                    if (replicator != null) {
                        survivalReplicators.add(replicator);
                        logger.info("[Survive] replicator {}:{}", zookeeperValue.getIp(), zookeeperValue.getPort());
                    } else {
                        logger.info("[Survive] replicator null for {}:{}", zookeeperValue.getIp(), zookeeperValue.getPort());
                    }
                    break;
                }
            }
        }

        if(survivalReplicators.size() != childrenData.size()){
            throw new IllegalStateException(String.format("[children data not equal with survival replicators]%s, %s", childrenData, survivalReplicators));
        }

        InstanceActiveElectAlgorithm klea = instanceActiveElectAlgorithmManager.get(clusterId);
        Replicator activeKeeper = (Replicator) klea.select(clusterId, survivalReplicators);  //set master
        currentMetaManager.setSurviveReplicators(clusterId, survivalReplicators, activeKeeper);
    }

    private Replicator getReplicator(String clusterId, String ip, int port) {
        DbCluster dbCluster = regionCache.getCluster(clusterId);
        List<Replicator> replicatorList = dbCluster.getReplicators();
        return replicatorList.stream().filter(replicator -> replicator.getIp().equalsIgnoreCase(ip) && replicator.getPort() == port).findFirst().orElse(null);
    }
}
