package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public abstract class AbstractCurrentMetaObserver extends AbstractLifecycleObservable implements Observer {

    @Autowired
    protected CurrentMetaManager currentMetaManager;

    @Autowired
    protected CurrentClusterServer currentClusterServer;

    @Autowired
    protected ClusterServerStateManager clusterServerStateManager;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        currentMetaManager.addObserver(this);
        clusterServerStateManager.addObserver(this);
    }

    @Override
    protected void doDispose() throws Exception {

        currentMetaManager.removeObserver(this);
        clusterServerStateManager.removeObserver(this);
        super.doDispose();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void update(Object args, Observable observable) {

        if(args instanceof NodeAdded){
            DbCluster clusterMeta = (DbCluster)((NodeAdded)args).getNode();
            logger.info("[update][add][{}]{}", getClass().getSimpleName(), clusterMeta.getId());
            handleClusterAdd(clusterMeta);  //add watcher to zk path
            return;
        }

        if(args instanceof NodeDeleted){
            DbCluster clusterMeta = (DbCluster)((NodeDeleted)args).getNode();
            logger.info("[update][delete][{}]{}", getClass().getSimpleName(), clusterMeta.getId());
            handleClusterDeleted(clusterMeta);
            return;
        }

        if(args instanceof ClusterComparator){
            ClusterComparator clusterMetaComparator = (ClusterComparator)args;
            logger.info("[update][modify][{}]{}", getClass().getSimpleName(), clusterMetaComparator);
            handleClusterModified(clusterMetaComparator);
            return;
        }

        if (args instanceof ServerStateEnum serverStateEnum) {
            logger.info("[update][changed][{}]{}", getClass().getSimpleName(), serverStateEnum);
            handleServerStateChanged(serverStateEnum);
        }

        throw new IllegalArgumentException("unknown argument:" + args);
    }

    protected void handleServerStateChanged(ServerStateEnum serverStateEnum) {
    }

    protected abstract void handleClusterModified(ClusterComparator comparator);

    protected abstract void handleClusterDeleted(DbCluster clusterMeta);

    protected abstract void handleClusterAdd(DbCluster clusterMeta);

    public void setCurrentMetaManager(CurrentMetaManager currentMetaManager) {
        this.currentMetaManager = currentMetaManager;
    }

    @Override
    public int getOrder() {
        return CurrentClusterServer.ORDER + 1;
    }
}
