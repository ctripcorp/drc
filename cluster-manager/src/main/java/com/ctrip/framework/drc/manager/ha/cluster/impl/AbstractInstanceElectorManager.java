package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.*;

/**
 * watch leader elector path and start instance
 *
 * @Author limingdong
 * @create 2020/4/29
 */
public abstract class AbstractInstanceElectorManager extends AbstractCurrentMetaObserver implements InstanceElectorManager, Observer, TopElement {

    public static final int FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI = 50;

    @Autowired
    private ZkClient zkClient;

    @Autowired
    protected RegionCache regionCache;

    @Autowired
    protected InstanceActiveElectAlgorithmManager instanceActiveElectAlgorithmManager;

    @Autowired
    protected ClusterManagerConfig clusterManagerConfig;

    private Map<String, PathChildrenCache> pathChildrenCacheMap = Maps.newConcurrentMap();

    private void observeLeader(final DbCluster cluster) {

        logger.info("[observeLeader]{}", cluster.getId());
        observerClusterLeader(cluster.getId());
    }

    @Override
    protected void handleServerStateChanged(ServerStateEnum serverStateEnum) {
        if (serverStateEnum.notAlive()) {
            logger.info("[handleServerStateChanged] not alive do nothing: {}, {}", serverStateEnum, this.getClass().getSimpleName());
            return;
        }
        if (CollectionUtils.isEmpty(pathChildrenCacheMap)) {
            logger.info("[handleServerStateChanged] pathChildrenCaches empty, {}", this.getClass().getSimpleName());
        } else {
            logger.info("[handleServerStateChanged] try to close pathChildrenCaches, size: {}, {}", pathChildrenCacheMap.size(), this.getClass().getSimpleName());
            for (Map.Entry<String, PathChildrenCache> entry : pathChildrenCacheMap.entrySet()) {
                try {
                    logger.info("[handleServerStateChanged][release children cache]: {}", entry.getKey());
                    entry.getValue().close();
                } catch (Exception e) {
                    logger.error("[handleServerStateChanged][release children cache] error: {}", entry.getKey(), e);
                }
            }
            pathChildrenCacheMap.clear();
        }

    }

    /**
     * registryKey is diff:
     * replicator: name.mha
     * applier: name.mha.dstMha[.dstDB]
     * messenger: name.mha._drc_mq[.dstDB]
     */
    protected void observerClusterLeader(final String registryKey) {

        logger.info("[observerShardLeader]{}", registryKey);

        final String leaderLatchPath = getLeaderPath(registryKey);
        final CuratorFramework client = zkClient.get();

        if (watchIfNotWatched(registryKey)) {
            try {
                logger.info("[observerClusterLeader][add PathChildrenCache]{}", registryKey);
                PathChildrenCache pathChildrenCache = new PathChildrenCache(client, leaderLatchPath, true, XpipeThreadFactory.create(String.format("PathChildrenCache:%s-%s", getType(), registryKey)));
                pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

                    private AtomicBoolean isFirst = new AtomicBoolean(true);

                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                        if (isFirst.get()) {
                            isFirst.set(false);
                            try {
                                logger.info("[childEvent][first sleep]{}", FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                                TimeUnit.MILLISECONDS.sleep(FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
                            } catch (Exception e) {
                                logger.error("[childEvent]", e);
                            }
                        }

                        logger.info("[childEvent]{}, {}, {}", registryKey, event.getType(), ZkUtils.toString(event.getData()));
                        if (CONNECTION_SUSPENDED == event.getType() || CONNECTION_RECONNECTED == event.getType() || CONNECTION_LOST == event.getType()) {
                            return;
                        }
                        updateClusterLeader(leaderLatchPath, pathChildrenCache.getCurrentData(), registryKey);
                    }
                });
                currentMetaManager.addResource(registryKey, new Releasable() {
                    @Override
                    public void release() throws Exception {
                        try {
                            if (clusterServerStateManager.getServerState().notAlive()) {
                                logger.info("[release][release children cache] server state not alive {}", registryKey);
                                pathChildrenCacheMap.put(registryKey, pathChildrenCache);
                            } else {
                                logger.info("[release][release children cache] finished {}", registryKey);
                                // Calling this method will handle the process in the background.
                                // When disconnected from ZK, it will continuously loop.
                                // If it does not reconnect with ZK for a long time, it may lead to an OOM error.
                                // For more details, please refer to CuratorFrameworkImpl#backgroundOperationsLoop()
                                pathChildrenCache.close();
                            }
                        } catch (Exception e) {
                            logger.error(String.format("[release][release children cache]%s", registryKey), e);
                        }
                    }
                });
                pathChildrenCache.start();
            } catch (Exception e) {
                logger.error("[observerShardLeader]" + registryKey, e);
            }
        } else {
            logger.warn("[observerShardLeader][already watched]{}", registryKey);
        }
    }

    protected LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    protected void updateClusterLeader(String leaderLatchPath, List<ChildData> childrenData, String registryKey) {
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

    @VisibleForTesting
    protected void addPathChildrenCache(String registryKey, PathChildrenCache pathChildrenCache) {
        pathChildrenCacheMap.put(registryKey, pathChildrenCache);
    }

    protected abstract String getLeaderPath(String registryKey);

    protected abstract String getType();

    protected abstract boolean watchIfNotWatched(String registryKey);
}
