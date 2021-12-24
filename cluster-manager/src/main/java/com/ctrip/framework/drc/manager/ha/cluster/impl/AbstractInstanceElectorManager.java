package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CONNECTION_LOST;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED;

/**
 * watch leader elector path and start instance
 * @Author limingdong
 * @create 2020/4/29
 */
public abstract class AbstractInstanceElectorManager extends AbstractCurrentMetaObserver implements InstanceElectorManager, Observer, TopElement {

    public static final int  FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI = 50;

    @Autowired
    private ZkClient zkClient;

    @Autowired
    protected DcCache dcCache;

    @Autowired
    protected InstanceActiveElectAlgorithmManager instanceActiveElectAlgorithmManager;

    private void observeLeader(final DbCluster cluster) {

        logger.info("[observeLeader]{}", cluster.getId());
        observerClusterLeader(cluster.getId());
    }

    protected void observerClusterLeader(final String clusterId) {

        logger.info("[observerShardLeader]{}", clusterId);

        final String leaderLatchPath = getLeaderPath(clusterId);
        final CuratorFramework client = zkClient.get();

        if(watchIfNotWatched(clusterId)){
            try {
                logger.info("[observerClusterLeader][add PathChildrenCache]{}", clusterId);
                PathChildrenCache pathChildrenCache = new PathChildrenCache(client, leaderLatchPath, true, XpipeThreadFactory.create(String.format("PathChildrenCache:%s-%s", getType(), clusterId)));
                pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

                    private AtomicBoolean isFirst = new AtomicBoolean(true);
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                        if(isFirst.get()){
                            isFirst.set(false);
                            try {
                                logger.info("[childEvent][first sleep]{}", FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                                TimeUnit.MILLISECONDS.sleep(FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                            }catch (Exception e){
                                logger.error("[childEvent]", e);
                            }
                        }

                        logger.info("[childEvent]{}, {}, {}", clusterId, event.getType(), ZkUtils.toString(event.getData()));
                        if (CONNECTION_SUSPENDED == event.getType() || CONNECTION_RECONNECTED == event.getType() || CONNECTION_LOST == event.getType()) {
                            return;
                        }
                        updateClusterLeader(leaderLatchPath, pathChildrenCache.getCurrentData(), clusterId);
                    }
                });
                currentMetaManager.addResource(clusterId, new Releasable() {
                    @Override
                    public void release() throws Exception {

                        try{
                            logger.info("[release][release children cache]{}", clusterId);
                            pathChildrenCache.close();
                        }catch (Exception e){
                            logger.error(String.format("[release][release children cache]%s", clusterId), e);
                        }
                    }
                });
                pathChildrenCache.start();
            } catch (Exception e) {
                logger.error("[observerShardLeader]" + clusterId, e);
            }
        } else{
            logger.warn("[observerShardLeader][already watched]{}", clusterId);
        }
    }

    protected LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    protected void updateClusterLeader(String leaderLatchPath, List<ChildData> childrenData, String clusterId){
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {
    }

    @Override
    protected void handleClusterAdd(DbCluster clusterMeta) {  //add whole DbCluster
        try {
            observeLeader(clusterMeta);
        } catch (Exception e) {
            logger.error("[handleClusterAdd]" + clusterMeta.getId(), e);
        }
    }

    @Override
    protected void handleClusterDeleted(DbCluster clusterMeta) {
        //nothing to do
    }

    protected abstract String getLeaderPath(String clusterId);

    protected abstract String getType();

    protected abstract boolean watchIfNotWatched(String clusterId);
}
