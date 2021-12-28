package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.ha.cluster.ApplierMasterElector;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.multidc.ApplierMasterChooser;
import com.ctrip.framework.drc.manager.ha.multidc.DefaultDcApplierMasterChooser;
import com.ctrip.framework.drc.manager.ha.multidc.MultiDcService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author limingdong
 * @create 2020/5/17
 */
@Component
public class DefaultApplierMasterChooserManager extends AbstractCurrentMetaObserver implements ApplierMasterElector, TopElement {

    @Autowired
    protected DcCache dcMetaCache;

    @Autowired
    private MultiDcService multiDcService;

    @Autowired
    private ClusterManagerConfig clusterManagerConfig;

    private ScheduledExecutorService scheduled;

    private Map<Key, ApplierMasterChooser> applierMasterChoosers = new ConcurrentHashMap<>();

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("DefaultApplierMasterChooserManager"));
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {
        logger.info("[handleClusterModified]{}", comparator.getFuture().getId());
        doHandleClusterChange(comparator.getFuture());
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        logger.info("[handleClusterDeleted]{}", dbCluster.getId());
        for (Applier applier : dbCluster.getAppliers()) {
            String targetMhaName = applier.getTargetMhaName();
            String targetIdc = applier.getTargetIdc();
            if (StringUtils.isNotBlank(targetMhaName) && StringUtils.isNotBlank(targetIdc)) {
                applierMasterChoosers.remove(new Key(targetIdc, targetMhaName));
            }
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {
        logger.info("[handleClusterAdd]{}", dbCluster.getId());
        doHandleClusterChange(dbCluster);
    }

    private void doHandleClusterChange(DbCluster dbCluster) {
        String replicatorMhaName = dbCluster.getMhaName();
        String clusterName = dbCluster.getName();
        List<Applier> applierList = dbCluster.getAppliers();
        Set<Key> targetMhaNameSet = Sets.newHashSet();

        logger.info("[doHandleClusterChange]{}, {}, {}", replicatorMhaName, clusterName, applierList.size());
        for (Applier applier : applierList) {
            String targetMhaName = applier.getTargetMhaName();
            String targetIdc = applier.getTargetIdc();
            logger.info("[doHandleClusterChange]{}, {}", targetMhaName, targetIdc);
            if (StringUtils.isNotBlank(targetMhaName) && StringUtils.isNotBlank(targetIdc)) {
                targetMhaNameSet.add(new Key(targetIdc, targetMhaName));
            }
        }

        for (Key entry : targetMhaNameSet) {
            MapUtils.getOrCreate(applierMasterChoosers, entry, () -> addApplierMasterChooser(entry.getIdc(), RegistryKey.from(clusterName, replicatorMhaName), RegistryKey.from(clusterName, entry.getMha())));
        }
    }

    private ApplierMasterChooser addApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId) {
        ApplierMasterChooser applierMasterChooser = new DefaultDcApplierMasterChooser(targetIdc, clusterId, backupClusterId, multiDcService, currentMetaManager, clusterManagerConfig, scheduled);

        try {
            logger.info("[addApplier]{}, {}, {}, {}", targetIdc, clusterId, backupClusterId, applierMasterChooser);
            applierMasterChooser.start();
            //release resources
            currentMetaManager.addResource(clusterId, applierMasterChooser);
        } catch (Exception e) {
            logger.error("[addApplier]{}, {}", clusterId, backupClusterId);
        }

        return applierMasterChooser;
    }

    @Override
    protected void doDispose() throws Exception {
        scheduled.shutdownNow();
        super.doDispose();
    }

    static class Key {
        String idc;
        String mha;

        public Key(String idc, String mha) {
            this.idc = idc;
            this.mha = mha;
        }

        public String getIdc() {
            return idc;
        }

        public String getMha() {
            return mha;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key key = (Key) o;
            return Objects.equals(idc, key.idc) &&
                    Objects.equals(mha, key.mha);
        }

        @Override
        public int hashCode() {

            return Objects.hash(idc, mha);
        }
    }

    @VisibleForTesting
    public Map<Key, ApplierMasterChooser> getApplierMasterChoosers() {
        return applierMasterChoosers;
    }
}
