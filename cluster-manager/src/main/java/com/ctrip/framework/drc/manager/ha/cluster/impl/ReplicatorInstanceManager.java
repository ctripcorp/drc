package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparatorVisitor;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ReplicatorComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    protected class ReplicatorChecker extends InstancePeriodicallyChecker<Replicator, ReplicatorInfoDto> {

        @Override
        protected Pair<List<String>, List<ReplicatorInfoDto>> fetchInstanceInfo(List<Instance> instances) {
            return batchInfoInquirer.getReplicatorInfo(instances);
        }

        @Override
        protected List<Instance> getAllMeta() {
            return Lists.newArrayList(currentMetaManager.getAllReplicatorInstances());
        }

        @Override
        protected Map<String, List<Replicator>> getMetaGroupByRegistryKeyMap() {
            return currentMetaManager.getAllMetaReplicator();
        }

        @Override
        protected Set<String> getAllRegistryKey(Map<String, List<Replicator>> metaGroupByRegistryKeyMap, Map<String, List<ReplicatorInfoDto>> instanceGroupByRegistryKey) {
            // ignore registry keys fetched from instance (when re-balance cluster, it could lead to false removal.)
            return Sets.newHashSet(metaGroupByRegistryKeyMap.keySet());
        }

        @Override
        protected boolean isUpstreamIpMatch(Replicator replicatorMaster, Db dbMaster, List<ReplicatorInfoDto> replicatorInfoDtos) {
            boolean upstreamMasterMatch = true;
            if (dbMaster == null || replicatorMaster == null) {
                return true;
            }
            String dbMasterIp = dbMaster.getIp();
            String replicatorMasterIp = replicatorMaster.getIp();
            for (ReplicatorInfoDto replicatorInfoDto : replicatorInfoDtos) {
                String expected = replicatorMasterIp;
                if (Boolean.TRUE.equals(replicatorInfoDto.getMaster())) {
                    expected = dbMasterIp;
                }
                if (!expected.equals(replicatorInfoDto.getUpstreamMasterIp())) {
                    return false;
                }
            }
            return upstreamMasterMatch;
        }

        @Override
        public String getName() {
            return "replicator";
        }

        @Override
        protected Replicator getReplicatorMaster(String clusterId, List<Replicator> instanceMetas) {
            return currentMetaManager.getActiveReplicator(clusterId);
        }

        @Override
        protected void removeRedundantInstance(String registryKey, String clusterId, Instance replicator) {
            // replicator should operate manually, no auto remove.
        }

        @Override
        void registerInstance(String clusterId, Replicator replicator) {
            instanceStateController.registerReplicator(clusterId, replicator);
        }

        @Override
        void refreshInstance(String clusterId, Replicator master) {
            instanceStateController.addReplicator(clusterId, master);
        }
    }

    public ReplicatorChecker getChecker() {
        return new ReplicatorChecker();
    }
}
