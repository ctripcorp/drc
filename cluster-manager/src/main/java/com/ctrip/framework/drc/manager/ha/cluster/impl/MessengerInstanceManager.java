package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparatorVisitor;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.MessengerComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.MessengerPropertyComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;

/**
 * Created by jixinwang on 2022/11/1
 */
@Component
public class MessengerInstanceManager extends AbstractInstanceManager implements TopElement {

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();

        MessengerComparator messengerComparator = comparator.getMessengerComparator();
        messengerComparator.accept(new MessengerInstanceManager.MessengerComparatorVisitor(clusterId));
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        for (Messenger messenger : dbCluster.getMessengers()) {
            removeMessenger(clusterId, messenger);
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {

        String clusterId = dbCluster.getId();
        List<Messenger> messengers = dbCluster.getMessengers();
        for (Messenger messenger : messengers) {
            registerMessenger(clusterId, messenger);
        }
    }

    private void registerMessenger(String clusterId, Messenger messenger) {
        try {
            instanceStateController.registerMessenger(clusterId, messenger);
        } catch (Exception e) {
            logger.error(String.format("[registerMessenger]%s,%s", clusterId, messenger), e);
        }
    }

    private void removeMessenger(String clusterId, Messenger messenger) {
        try {
            instanceStateController.removeMessenger(clusterId, messenger, true);
        } catch (Exception e) {
            logger.error(String.format("[addMessenger]%s,%s", clusterId, messenger), e);
        }
    }

    public class MessengerComparatorVisitor implements MetaComparatorVisitor<Messenger> {

        private String clusterId;

        public MessengerComparatorVisitor(String clusterId) {
            this.clusterId = clusterId;
        }

        @Override
        public void visitAdded(Messenger added) {
            logger.info("[visitAdded][add Messenger]{}", added);
            registerMessenger(clusterId, added);
        }

        @Override
        public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {
            if (!clusterManagerConfig.checkApplierProperty()) {
                logger.info("[visitModified][messengerPropertyChange] ignore ");
                return;
            }

            MessengerPropertyComparator propertyComparator = (MessengerPropertyComparator) comparator;
            Messenger current = (Messenger) propertyComparator.getCurrent();
            Messenger future = (Messenger) propertyComparator.getFuture();
            logger.info("[visitModified][messengerPropertyChange]{} to {}", current, future);
            Set<Messenger> messengers = propertyComparator.getAdded();
            if (messengers.isEmpty()) {
                logger.info("[visitModified][messengerPropertyChange] do nothing");
                return;
            }

            for (Messenger modified : messengers) {
                String dbName = NameUtils.getMessengerDbName(modified);
                Messenger activeMessenger = currentMetaManager.getActiveMessenger(clusterId, dbName);
                if (modified.equalsWithIpPort(activeMessenger) && ObjectUtils.equals(modified.getIncludedDbs(), activeMessenger.getIncludedDbs())) {
                    activeMessenger.setNameFilter(modified.getNameFilter());
                    activeMessenger.setProperties(modified.getProperties());
                    logger.info("[visitModified][messengerPropertyChange] clusterId: {}, activeMessenger: {}", clusterId, activeMessenger);
                    instanceStateController.messengerPropertyChange(clusterId, activeMessenger);
                }
            }
        }

        @Override
        public void visitRemoved(Messenger removed) {
            logger.info("[visitRemoved][remove Messenger]{}", removed);
            removeMessenger(clusterId, removed);

        }
    }

    protected class MessengerChecker extends InstancePeriodicallyChecker<Messenger, ApplierInfoDto> {
        @Override
        protected Pair<List<String>, List<ApplierInfoDto>> fetchInstanceInfo(List<Instance> instances) {
            return instanceStateController.getMessengerInfo(instances);
        }

        @Override
        protected List<Instance> getAllMeta() {
            return Lists.newArrayList(currentMetaManager.getAllApplierOrMessengerInstances());
        }

        @Override
        protected Map<String, List<Messenger>> getMetaGroupByRegistryKeyMap() {
            Map<String, Map<String, List<Messenger>>> allSurviveMessengers = currentMetaManager.getAllMetaMessengers();
            Map<String, List<Messenger>> messengerMetaGroupByRegistryKeyMap = new HashMap<>();
            for (Map.Entry<String, Map<String, List<Messenger>>> entry : allSurviveMessengers.entrySet()) {
                String clusterId = entry.getKey();
                Map<String, List<Messenger>> messengerGroupByBackupKey = entry.getValue();
                for (Map.Entry<String, List<Messenger>> en : messengerGroupByBackupKey.entrySet()) {
                    String dbName = en.getKey();
                    messengerMetaGroupByRegistryKeyMap.put(NameUtils.getMessengerRegisterKey(clusterId, dbName), en.getValue());
                }
            }
            return messengerMetaGroupByRegistryKeyMap;
        }

        @Override
        public String getName() {
            return "messenger";
        }

        @Override
        protected Replicator getReplicatorMaster(String clusterId, List<Messenger> instanceMetas) {
            return currentMetaManager.getActiveReplicator(clusterId);
        }

        @Override
        protected void removeRedundantInstance(String registryKey, String clusterId, Instance messenger) {
            String targetDB = NameUtils.getMessengerDbName(registryKey);
            ApplyMode applyMode = DRC_MQ.equals(targetDB) ? ApplyMode.mq : ApplyMode.db_mq;

            Messenger messengerToRemove = new Messenger().setIp(messenger.getIp()).setPort(messenger.getPort()).setMaster(messenger.getMaster())
                    .setIncludedDbs(targetDB).setApplyMode(applyMode.getType());
            removeMessenger(clusterId, messengerToRemove);
        }


        @Override
        void registerInstance(String clusterId, Messenger messenger) {
            instanceStateController.registerMessenger(clusterId, messenger);
        }

        @Override
        void refreshInstance(String clusterId, Messenger master) {
            instanceStateController.addMessenger(clusterId, master);
        }

    }

    public MessengerChecker getChecker() {
        return new MessengerChecker();
    }
}
