package com.ctrip.framework.drc.manager.ha;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.ha.cluster.impl.InstanceStateController;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
@Order(0)
@Component
public class DefaultStateChangeHandler extends AbstractLifecycle implements StateChangeHandler, TopElement {

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private InstanceStateController instanceStateController;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
    }

    @Override
    public void replicatorActiveElected(String clusterId, Replicator replicator) {
        STATE_LOGGER.info("[replicatorActiveElected]{},{}", clusterId, replicator);

        List<Replicator> replicators = currentMetaManager.getSurviveReplicators(clusterId);
        if (replicators == null || replicators.size() == 0) {
            STATE_LOGGER.info("[{}][replicatorActiveElected][none replicator survive, do nothing]", getClass().getSimpleName());
            return;
        }

        if (currentMetaManager.hasCluster(clusterId)) {
            DbCluster dbCluster = currentMetaManager.getCluster(clusterId);
            if (dbCluster == null) {
                STATE_LOGGER.info("[replicatorActiveElected][not interested, do nothing]");
                return;
            }

            instanceStateController.addReplicator(clusterId, replicator);  // notify all replicators
            STATE_LOGGER.info("[Notify] add replicator dbCluster {}", clusterId);
        } else {
            STATE_LOGGER.info("[replicatorActiveElected][not interested, do nothing]");
        }

    }

    @Override
    public void applierActiveElected(String clusterId, Applier applier) {
        STATE_LOGGER.info("[applierActiveElected]{},{}", clusterId, applier);

        List<Applier> appliers = currentMetaManager.getSurviveAppliers(clusterId, RegistryKey.from(applier.getTargetName(), applier.getTargetMhaName()));
        if (appliers == null || appliers.size() == 0) {
            STATE_LOGGER.info("[applierActiveElected][none applier survive, do nothing]");
            return;
        }

        if (currentMetaManager.hasCluster(clusterId)) {
            DbCluster dbCluster = currentMetaManager.getCluster(clusterId);
            if (dbCluster == null) {
                STATE_LOGGER.info("[applierActiveElected][not interested, do nothing]");
                return;
            }

            instanceStateController.addApplier(clusterId, applier);
            STATE_LOGGER.info("[Notify] add applier dbCluster {}", clusterId);
        } else {
            STATE_LOGGER.info("[applierActiveElected][not interested, do nothing]");
        }
    }

    @Override
    public void applierMasterChanged(String clusterId, String backupClusterId, Pair<String, Integer> newMaster) {
        STATE_LOGGER.info("[applierMasterChanged]{},{},{}", clusterId, backupClusterId, newMaster);
        Applier activeApplier = currentMetaManager.getActiveApplier(clusterId, backupClusterId);

        if (activeApplier == null) {  //when add new applier, chooser get replicator but not register, then return. when applier register, pick up active one to add
            STATE_LOGGER.info("[applierMasterChanged][no active applier, do nothing]{},{},{}", clusterId, backupClusterId, newMaster);
            return;
        }
        if (!activeApplier.isMaster()) {
            throw new IllegalStateException("[active applier not active]{}" + activeApplier);
        }

        STATE_LOGGER.info("[applierMasterChanged][set active applier master]{}, {}", activeApplier, newMaster);
        instanceStateController.applierMasterChange(clusterId, newMaster, activeApplier);
    }

    @Override
    public void mysqlMasterChanged(String clusterId, Endpoint endpoint) {
        STATE_LOGGER.info("[mysqlMasterChange][set mysql master]{}, {}", clusterId, endpoint);
        List<Applier> activeApplier = currentMetaManager.getActiveAppliers(clusterId);
        for (Applier applier : activeApplier) {
            if (!check(applier)) {
                return;
            }
        }

        Replicator replicator = currentMetaManager.getActiveReplicator(clusterId);
        if (!check(replicator)) {
            return;
        }
        instanceStateController.mysqlMasterChanged(clusterId, endpoint, activeApplier, replicator);
    }

    private boolean check(Instance instance) {
        if (instance == null) {
            STATE_LOGGER.info("[no active instance");
            return false;
        }
        if (!instance.getMaster()) {
            STATE_LOGGER.info("[active instance not active]{},{}", instance.getIp(), instance.getPort());
            return false;
        }
        return true;
    }
}
