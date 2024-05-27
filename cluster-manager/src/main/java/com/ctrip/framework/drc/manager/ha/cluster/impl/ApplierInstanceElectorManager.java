package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.Constants;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.container.ZookeeperValue;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.config.ClusterZkConfig;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ApplierComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * @Author limingdong
 * @create 2020/5/7
 */
@Component
public class ApplierInstanceElectorManager extends AbstractInstanceElectorManager implements InstanceElectorManager, Observer, TopElement {

    @Autowired
    private InstanceStateController instanceStateController;

    @Override
    protected String getLeaderPath(String registryKey) {
        return ClusterZkConfig.getApplierLeaderLatchPath(registryKey);
    }

    @Override
    protected String getType() {
        return Constants.ENTITY_APPLIER;
    }

    @Override
    protected boolean watchIfNotWatched(String registryKey) {
        return currentMetaManager.watchApplierIfNotWatched(registryKey);
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {
        String clusterId = dbCluster.getId();
        try {
            List<Applier> appliers = dbCluster.getAppliers();
            doObserveLeader(clusterId, appliers);
        } catch (Exception e) {
            logger.error("[handleClusterAdd]" + clusterId, e);
        }
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();
        ApplierComparator applierComparator = comparator.getApplierComparator();
        Set<Applier> addedApplier = applierComparator.getAdded();
        doObserveLeader(clusterId, addedApplier);
    }

    private void doObserveLeader(String clusterId, Collection<Applier> appliers) {
        for (Applier applier : appliers) {
            String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
            observerClusterLeader(registryKey);
        }
    }

    protected void updateClusterLeader(String leaderLatchPath, List<ChildData> childrenData, String registryKey) {

        String clusterId = RegistryKey.from(registryKey).toString();
        logger.info("[Transfer][applier] {} to {}", registryKey, clusterId);
        List<String> childrenPaths = new LinkedList<>();
        childrenData.forEach(childData -> childrenPaths.add(childData.getPath()));

        logger.info("[updateClusterLeader][applier]{}, {}", registryKey, childrenPaths);

        List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, childrenPaths);

        List<Applier> survivalAppliers = new ArrayList<>(childrenData.size());
        List<Applier> redundantAppliers = new ArrayList<>();

        String targetMha = RegistryKey.getTargetMha(registryKey);
        String targetDB = RegistryKey.getTargetDB(registryKey);

        for (String path : sortedChildren) {
            for (ChildData childData : childrenData) {
                if (path.equals(childData.getPath())) {
                    String data = new String(childData.getData());
                    ZookeeperValue zookeeperValue = JsonCodec.INSTANCE.decode(data, ZookeeperValue.class);
                    Applier applier = getApplier(clusterId, zookeeperValue.getIp(), zookeeperValue.getPort(), targetMha, targetDB);
                    if (applier != null) {
                        survivalAppliers.add(applier);
                        logger.info("[Survive] applier {} {}:{}", clusterId, zookeeperValue.getIp(), zookeeperValue.getPort());
                    } else {
                        // exist in zk but not in memory, should remove
                        redundantAppliers.add(new Applier()
                                .setIp(zookeeperValue.getIp()).setPort(zookeeperValue.getPort())
                                .setTargetMhaName(targetMha)
                                .setIncludedDbs(targetDB).setApplyMode(StringUtils.isBlank(targetDB) ? ApplyMode.transaction_table.getType() : ApplyMode.db_transaction_table.getType())
                        );
                        logger.warn("[Survive] applier null for {} {}:{}", clusterId, zookeeperValue.getIp(), zookeeperValue.getPort());
                    }
                    break;
                }
            }
        }

        if (!CollectionUtils.isEmpty(redundantAppliers)) {
            logger.warn("do remove applier: {}", redundantAppliers);
            redundantAppliers.forEach(redundantApplier -> removeApplier(clusterId, redundantApplier));
        }

        if (survivalAppliers.size() != childrenData.size()) {
            throw new IllegalStateException(String.format("[children data not equal with survival appliers]%s, %s", childrenData, survivalAppliers));
        }

        InstanceActiveElectAlgorithm klea = instanceActiveElectAlgorithmManager.get(clusterId);
        Applier activeApplier = (Applier) klea.select(clusterId, survivalAppliers);  //set master
        currentMetaManager.setSurviveAppliers(clusterId, registryKey, survivalAppliers, activeApplier);
    }

    private Applier getApplier(String clusterId, String ip, int port, String targetMha, String targetDB) {
        DbCluster dbCluster = regionCache.getCluster(clusterId);
        logger.info("[DbCluster] for applier is {}", dbCluster);
        List<Applier> applierList = dbCluster.getAppliers();
        return applierList.stream().filter(applier ->
                applier.getIp().equalsIgnoreCase(ip) && applier.getPort() == port
                        && applier.getTargetMhaName().equalsIgnoreCase(targetMha)
                        && (StringUtils.isBlank(targetDB) || ObjectUtils.equals(applier.getIncludedDbs(), targetDB)))
                .findFirst().orElse(null);
    }

    private void removeApplier(String clusterId, Applier applier) {
        try {
            EventMonitor.DEFAULT.logEvent("remove.redundant.zk.applier", NameUtils.getApplierRegisterKey(clusterId, applier) + ":" + applier.getIp());
            instanceStateController.removeApplier(clusterId, applier, true);
        } catch (Exception e) {
            logger.error(String.format("[removeApplier]%s,%s", clusterId, applier), e);
        }
    }
}
