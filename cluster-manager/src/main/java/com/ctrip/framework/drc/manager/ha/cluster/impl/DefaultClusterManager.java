package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.manager.ha.cluster.ClusterManager;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.rest.ForwardInfo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * invoke in controller, and update dcMetaCache, then notify metaCache observers to notify replicators and appliers
 * @Author limingdong
 * @create 2020/4/20
 */
@Component
public class DefaultClusterManager extends DefaultCurrentClusterServer implements ClusterManager {

    @Autowired
    private CurrentMetaManager currentMetaManager;   // managed by current cm

    @Autowired
    private RegionCache regionMetaCache;   //all in current dc

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        LifecycleHelper.initializeIfPossible(currentMetaManager);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        LifecycleHelper.startIfPossible(currentMetaManager);

    }

    @Override
    protected void doStop() throws Exception {

        LifecycleHelper.stopIfPossible(currentMetaManager);
        super.doStop();
    }

    @Override
    protected void doDispose() throws Exception {

        LifecycleHelper.disposeIfPossible(currentMetaManager);
        super.doDispose();
    }

    public void setConfig(ClusterManagerConfig config) {
        this.config = config;
    }

    @Override
    protected void doSlotAdd(int slotId) {

        super.doSlotAdd(slotId);
        currentMetaManager.addSlot(slotId);
    }

    @Override
    protected void doSlotDelete(int slotId) {
        super.doSlotDelete(slotId);

        currentMetaManager.deleteSlot(slotId);
    }

    @Override
    protected void doSlotExport(int slotId) {
        super.doSlotExport(slotId);
        currentMetaManager.exportSlot(slotId);
    }

    @Override
    protected void doSlotImport(int slotId) {
        super.doSlotImport(slotId);
        currentMetaManager.importSlot(slotId);
    }

    @Override
    public String getCurrentMeta() {
        return currentMetaManager.getCurrentMetaDesc();
    }

    @Override
    public void clusterAdded(String dcId, DbCluster clusterMeta, ForwardInfo forwardInfo) {
        logger.info("[clusterAdded]{}", clusterMeta);
        regionMetaCache.clusterAdded(dcId, clusterMeta);
    }

    @Override
    public void clusterModified(DbCluster clusterMeta, ForwardInfo forwardInfo) {
        logger.info("[clusterModified]{}", clusterMeta);
        regionMetaCache.clusterModified(clusterMeta);

    }

    @Override
    public void clusterDeleted(String clusterId, ForwardInfo forwardInfo) {

        logger.info("[clusterDeleted]{}", clusterId);
        regionMetaCache.clusterDeleted(clusterId);
    }

    @Override
    public void updateUpstream(String clusterId, String backupClusterId, String ip, int port, ForwardInfo forwardInfo) {
        logger.info("[updateUpstream]{},{},{},{}", clusterId, backupClusterId, ip, port);
        currentMetaManager.setApplierMaster(clusterId, backupClusterId, ip, port);
    }

    @Override
    public Replicator getActiveReplicator(String clusterId, ForwardInfo forwardInfo) {
        logger.debug("[getActiveReplicator]{}", clusterId);
        return currentMetaManager.getActiveReplicator(clusterId);
    }

    @Override
    public Endpoint getActiveMySQL(String clusterId, ForwardInfo forwardInfo) {
        logger.debug("[getActiveMySQL]{}", clusterId);
        return currentMetaManager.getMySQLMaster(clusterId);
    }
}
