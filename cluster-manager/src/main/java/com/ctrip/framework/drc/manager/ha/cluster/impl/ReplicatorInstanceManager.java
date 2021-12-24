package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparatorVisitor;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ReplicatorComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author limingdong
 * @create 2020/5/6
 */
@Component
public class ReplicatorInstanceManager extends AbstractInstanceManager implements TopElement {

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();

        ReplicatorComparator replicatorComparator = comparator.getReplicatorComparator();
        replicatorComparator.accept(new ReplicatorComparatorVisitor(clusterId));
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        for (Replicator replicator : dbCluster.getReplicators()) {
            removeReplicator(clusterId, replicator);
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {

        String clusterId = dbCluster.getId();
        List<Replicator> replicators = dbCluster.getReplicators();
        for (Replicator replicator : replicators) {
            registerReplicator(clusterId, replicator);
        }
    }

    private void registerReplicator(String clusterId, Replicator replicator) {
        try {
            instanceStateController.registerReplicator(clusterId, replicator);
        } catch (Exception e) {
            logger.error(String.format("[registerReplicator]%s,%s", clusterId, replicator), e);
        }
    }

    private void removeReplicator(String clusterId, Replicator replicator) {
        try {
            instanceStateController.removeReplicator(clusterId, replicator);
        } catch (Exception e) {
            logger.error(String.format("[addReplicator]%s,%s", clusterId, replicator), e);
        }
    }

    public class ReplicatorComparatorVisitor implements MetaComparatorVisitor<Replicator> {

        private String clusterId;

        public ReplicatorComparatorVisitor(String clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public void visitAdded(Replicator added) {
            logger.info("[visitAdded][add Replicator]{}", added);
            registerReplicator(clusterId, added);
        }

        @Override
        public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {

        }

        @Override
        public void visitRemoved(Replicator removed) {
            logger.info("[visitRemoved][remove Replicator]{}", removed);
            removeReplicator(clusterId, removed);

        }
    }
}
