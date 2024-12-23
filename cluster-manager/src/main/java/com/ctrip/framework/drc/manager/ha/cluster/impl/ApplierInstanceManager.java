package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparatorVisitor;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ApplierComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ApplierPropertyComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @Author limingdong
 * @create 2020/5/6
 */
@Component
public class ApplierInstanceManager extends AbstractInstanceManager implements TopElement {


    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();
        ApplierComparator applierComparator = comparator.getApplierComparator();
        applierComparator.accept(new ApplierComparatorVisitor(clusterId));
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        for (Applier applier : dbCluster.getAppliers()) {
            removeApplier(clusterId, applier);
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        List<Applier> appliers = dbCluster.getAppliers();
        for (Applier applier : appliers) {
            registerApplier(clusterId, applier);
        }
    }

    private void registerApplier(String clusterId, Applier applier) {
        try {
            instanceStateController.registerApplier(clusterId, applier);
        } catch (Exception e) {
            logger.error(String.format("[addApplier]%s,%s", clusterId, applier), e);
        }
    }

    private void removeApplier(String clusterId, Applier applier) {
        try {
            instanceStateController.removeApplier(clusterId, applier, true);
        } catch (Exception e) {
            logger.error(String.format("[removeApplier]%s,%s", clusterId, applier), e);
        }
    }

    protected class ApplierComparatorVisitor implements MetaComparatorVisitor<Applier> {

        private String clusterId;

        public ApplierComparatorVisitor(String clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public void visitAdded(Applier added) {
            logger.info("[visitAdded][add shard]{}", added);
            registerApplier(clusterId, added);
        }

        @Override
        public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {
            if (!clusterManagerConfig.checkApplierProperty()) {
                logger.info("[visitModified][applierPropertyChange] ignore ");
                return;
            }

            ApplierPropertyComparator propertyComparator = (ApplierPropertyComparator) comparator;
            Applier current = (Applier) propertyComparator.getCurrent();
            Applier future = (Applier) propertyComparator.getFuture();
            logger.info("[visitModified][applierPropertyChange]{} to {}", current, future);
            Set<Applier> appliers = propertyComparator.getAdded();
            if (appliers.isEmpty()) {
                logger.info("[visitModified][applierPropertyChange] do nothing");
                return;
            }

            for (Applier modified : appliers) {
                String backupRegistryKey = NameUtils.getApplierBackupRegisterKey(modified);
                Applier activeApplier = currentMetaManager.getActiveApplier(clusterId, backupRegistryKey);
                if (modified.equalsWithIpPort(activeApplier) && ObjectUtils.equals(modified.getIncludedDbs(), activeApplier.getIncludedDbs())) {
                    activeApplier.setNameFilter(modified.getNameFilter());
                    activeApplier.setProperties(modified.getProperties());
                    logger.info("[visitModified][applierPropertyChange] clusterId: {}, backupClusterId: {}, activeApplier: {}", clusterId, backupRegistryKey, activeApplier);
                    instanceStateController.applierPropertyChange(clusterId, activeApplier);
                }
            }
        }

        @Override
        public void visitRemoved(Applier removed) {
            logger.info("[visitRemoved][remove shard]{}", removed);
            removeApplier(clusterId, removed);
        }
    }

    protected class ApplierChecker extends InstancePeriodicallyChecker<Applier, ApplierInfoDto> {
        @Override
        protected Pair<List<String>, List<ApplierInfoDto>> fetchInstanceInfo(List<Instance> instances) {
            return batchInfoInquirer.getApplierInfo(instances);
        }

        @Override
        protected List<Instance> getAllMeta() {
            return Lists.newArrayList(currentMetaManager.getAllApplierOrMessengerInstances());
        }

        @Override
        protected Map<String, List<Applier>> getMetaGroupByRegistryKeyMap() {
            Map<String, Map<String, List<Applier>>> allSurviveAppliers = currentMetaManager.getAllMetaAppliers();
            Map<String, List<Applier>> applierMetaGroupByRegistryKeyMap = new HashMap<>();
            for (Map.Entry<String, Map<String, List<Applier>>> entry : allSurviveAppliers.entrySet()) {
                String clusterId = entry.getKey();
                Map<String, List<Applier>> applierGroupByBackupKey = entry.getValue();
                for (Map.Entry<String, List<Applier>> en : applierGroupByBackupKey.entrySet()) {
                    String backupRegistryKey = en.getKey();
                    applierMetaGroupByRegistryKeyMap.put(NameUtils.getApplierRegisterKey(clusterId, backupRegistryKey), en.getValue());
                }
            }
            return applierMetaGroupByRegistryKeyMap;
        }

        @Override
        public String getName() {
            return "applier";
        }

        @Override
        protected Replicator getReplicatorMaster(String clusterId, List<Applier> applierMetas) {
            String backupRegistryKey = NameUtils.getApplierBackupRegisterKey(applierMetas.get(0));
            Pair<String, Integer> applierMaster = currentMetaManager.getApplierMaster(clusterId, backupRegistryKey);
            Replicator replicator = null;
            if (applierMaster != null) {
                replicator = new Replicator();
                replicator.setIp(applierMaster.getKey()).setPort(applierMaster.getValue());
            }
            return replicator;
        }


        @Override
        protected void removeRedundantInstance(String registryKey, String clusterId, Instance applier) {
            String targetMha = RegistryKey.getTargetMha(registryKey);
            String targetDB = RegistryKey.getTargetDB(registryKey);
            ApplyMode applyMode = StringUtils.isEmpty(targetDB) ? ApplyMode.transaction_table : ApplyMode.db_transaction_table;

            Applier applierToRemove = new Applier().setIp(applier.getIp()).setPort(applier.getPort()).setMaster(applier.getMaster())
                    .setTargetMhaName(targetMha).setIncludedDbs(targetDB).setApplyMode(applyMode.getType());
            removeApplier(clusterId, applierToRemove);
        }

        @Override
        protected boolean isDownStreamIpMatch(Replicator replicatorMaster, Db dbMaster, List<ApplierInfoDto> instances) {
            if (dbMaster == null) {
                return true;
            }

            return instances.stream()
                    .map(ApplierInfoDto::getDbInfo)
                    .filter(Objects::nonNull)
                    .allMatch(e -> dbMaster.getIp().equals(e.getIp()) && dbMaster.getPort().equals(e.getPort()));
        }

        @Override
        void registerInstance(String clusterId, Applier applier) {
            instanceStateController.registerApplier(clusterId, applier);
        }

        @Override
        void refreshInstance(String clusterId, Applier master) {
            instanceStateController.addApplier(clusterId, master);
        }
    }

    public ApplierChecker getChecker() {
        return new ApplierChecker();
    }
}
