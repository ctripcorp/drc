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
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.Set;
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

    protected ScheduledExecutorService scheduled;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("DefaultApplierMasterChooserManager"));
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {
        //TODO handle when multi dc applier
    }

    @Override
    protected void handleClusterDeleted(DbCluster dbCluster) {
        //nothing to do
    }

    @Override
    protected void handleClusterAdd(DbCluster dbCluster) {

        String replicatorMhaName = dbCluster.getMhaName();
        String clusterName = dbCluster.getName();
        List<Applier> applierList = dbCluster.getAppliers();
        Set<Key> targetMhaNameSet = Sets.newHashSet();

        logger.info("[handleClusterAdd]{}, {}, {}", replicatorMhaName, clusterName, applierList.size());
        for (Applier applier : applierList) {
            String targetMhaName = applier.getTargetMhaName();
            String targetIdc = applier.getTargetIdc();
            logger.info("[handleClusterAdd]{}, {}", targetMhaName, targetIdc);
            if (StringUtils.isNotBlank(targetMhaName) && StringUtils.isNotBlank(targetIdc)) {
                targetMhaNameSet.add(new Key(targetIdc, targetMhaName));
            }
        }

        for (Key entry : targetMhaNameSet) {
            addApplierMasterChooser(entry.getIdc(), RegistryKey.from(clusterName, replicatorMhaName), RegistryKey.from(clusterName, entry.getMha()));
        }

    }
    private void addApplierMasterChooser(String targetIdc, String clusterId, String backupClusterId) {

        ApplierMasterChooser keeperMasterChooser = new DefaultDcApplierMasterChooser(targetIdc, clusterId, backupClusterId, multiDcService, currentMetaManager, clusterManagerConfig, scheduled);


        try {
            logger.info("[addApplier]{}, {}, {}, {}", targetIdc, clusterId, backupClusterId, keeperMasterChooser);
            keeperMasterChooser.start();
            //release resources
            currentMetaManager.addResource(clusterId, keeperMasterChooser);
        } catch (Exception e) {
            logger.error("[addApplier]{}, {}", clusterId, backupClusterId);
        }
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
}
