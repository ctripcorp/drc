package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ApplierNotifier;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ReplicatorNotifier;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * dbcluster combined with copy from dcMetaCache and currentMetaManager
 * @Author limingdong
 * @create 2020/5/6
 */
@Component
public class DefaultInstanceStateController extends AbstractLifecycle implements InstanceStateController, TopElement {

    @Autowired
    private DcCache dcMetaCache;

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private ClusterManagerConfig config;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    /**
     * notify replicator to pull mysql binlog and slave to pull master binlog
     * @param clusterId
     * @param replicator
     * @return
     */
    @Override
    public DbCluster addReplicator(String clusterId, Replicator replicator) {
        ReplicatorNotifier replicatorNotifier = ReplicatorNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshReplicator(clusterId, replicator);
        List<Replicator> replicators = currentMetaManager.getSurviveReplicators(clusterId);
        for (Replicator r : replicators) {
            if (!r.equalsWithIpPort(replicator)) {
                body.getReplicators().add(r);
            }
        }
        STATE_LOGGER.info("[addReplicator] for {}", body);
        executors.submit(() -> replicatorNotifier.notifyAdd(body));
        return body;
    }

    /**
     * notify replicator to register zookeeper
     * @param clusterId
     * @param replicator
     * @return
     */
    @Override
    public DbCluster registerReplicator(String clusterId, Replicator replicator) {
        ReplicatorNotifier replicatorNotifier = ReplicatorNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshReplicator(clusterId, replicator);
        STATE_LOGGER.info("[registerReplicator] for {}", body);
        executors.submit(() -> replicatorNotifier.notifyRegister(body));
        return body;
    }

    /**
     * notify applier to register zookeeper
     * @param clusterId
     * @param applier
     * @return
     */
    @Override
    public DbCluster registerApplier(String clusterId, Applier applier) {
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshApplier(clusterId, applier);
        STATE_LOGGER.info("[registerApplier] for {}", body);
        executors.submit(() -> applierNotifier.notifyRegister(body));
        return body;
    }

    /**
     * notify active applier to pull replicator binlog
     * @param clusterId
     * @param applier
     * @return
     */
    @Override
    public DbCluster addApplier(String clusterId, Applier applier) {
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshApplier(clusterId, applier);
        STATE_LOGGER.info("[addApplier] for {}", body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty] replicators and do nothing for {}", clusterId);
            return body;
        }
        executors.submit(() -> applierNotifier.notifyAdd(body));

        List<Applier> appliers = currentMetaManager.getSurviveAppliers(clusterId);
        for (Applier a : appliers) {
            if (!a.equalsWithIpPort(applier) && a.getTargetMhaName().equalsIgnoreCase(applier.getTargetMhaName())) {
                removeApplier(clusterId, a, false);
            }
        }
        return body;
    }

    @Override
    public void removeReplicator(String clusterId, Replicator replicator) {
        if (config.getMigrationBlackIps().contains(replicator.getIp())) {
            logger.info("[skipRemove] black ips are: {}, replicator ip is:{}", config.getMigrationBlackIps(), replicator.getIp());
            return;
        }
        ReplicatorNotifier replicatorNotifier = ReplicatorNotifier.getInstance();
        STATE_LOGGER.info("[removeReplicator] for {},{}", clusterId, replicator);
        executors.submit(() -> replicatorNotifier.notifyRemove(clusterId, replicator, true));
    }

    @Override
    public void removeApplier(String clusterId, Applier applier, boolean delete) {
        if (config.getMigrationBlackIps().contains(applier.getIp())) {
            logger.info("[skipRemove] black ips are: {}, applier ip is:{}", config.getMigrationBlackIps(), applier.getIp());
            return;
        }
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        String targetMhaName = applier.getTargetMhaName();
        String newClusterId = RegistryKey.from(clusterId, targetMhaName);
        STATE_LOGGER.info("[removeApplier] for {},{}", newClusterId, applier);
        executors.submit(() -> applierNotifier.notifyRemove(newClusterId, applier, delete));
    }

    /**
     * notify active applier pull binlog from new active replicator
     * @param clusterId
     * @param newMaster
     * @param applier
     * @return
     */
    @Override
    public DbCluster applierMasterChange(String clusterId, Pair<String, Integer> newMaster, Applier applier) {  //notify by http
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshApplier(clusterId, applier);
        STATE_LOGGER.info("[applierMasterChange] for {}", body);
        executors.submit(() -> applierNotifier.notifyAdd(body));
        return body;
    }

    /**
     * notify replicator and applier to pull binlog from new mysql master
     * @param clusterId
     * @param mysqlMaster
     * @param activeApplier
     * @param replicator
     * @return
     */
    @Override
    public List<DbCluster> mysqlMasterChanged(String clusterId, Endpoint mysqlMaster, List<Applier> activeApplier, Replicator replicator) {
        List<DbCluster> res = Lists.newArrayList();
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        for (Applier applier : activeApplier) {
            DbCluster dbCluster = getDbClusterWithRefreshApplier(clusterId, applier, mysqlMaster);
            applierNotifier.notify(dbCluster);
            res.add(dbCluster);
        }

        ReplicatorNotifier replicatorNotifier = ReplicatorNotifier.getInstance();
        DbCluster dbCluster = getDbClusterWithRefreshReplicator(clusterId, replicator, mysqlMaster);
        dbCluster.getReplicators().clear();
        dbCluster.getReplicators().add(replicator);
        executors.submit(() -> replicatorNotifier.notify(dbCluster));
        res.add(dbCluster);
        STATE_LOGGER.info("[mysqlMasterChanged] for {}", res);

        return res;
    }

    private DbCluster getDbClusterWithRefreshReplicator(String clusterId, Replicator replicator) {
        return getDbClusterWithRefreshReplicator(clusterId, replicator, getMySQLMaster(clusterId));
    }

    private DbCluster getDbClusterWithRefreshReplicator(String clusterId, Replicator replicator, Endpoint mysqlMaster) {
        DbCluster dbCluster = dcMetaCache.getCluster(clusterId);
        DbCluster clone = MetaClone.clone(dbCluster);
        clone.getReplicators().clear();
        clone.getReplicators().add(replicator);

        if (mysqlMaster != null) {
            setMySQL(clone, mysqlMaster);
        }

        return clone;
    }

    private DbCluster getDbClusterWithRefreshApplier(String clusterId, Applier applier) {
        return getDbClusterWithRefreshApplier(clusterId, applier, getMySQLMaster(clusterId));
    }

    private DbCluster getDbClusterWithRefreshApplier(String clusterId, Applier applier, Endpoint mysqlMaster) {  //just notify applier for iterate outside
        String targetName = applier.getTargetName();
        String targetMhaName = applier.getTargetMhaName();
        String backupClusterId = RegistryKey.from(targetName, targetMhaName);
        DbCluster dbCluster = dcMetaCache.getCluster(clusterId);
        DbCluster clone = MetaClone.clone(dbCluster);
        clone.getAppliers().clear();
        clone.getAppliers().add(applier);

        //add replicator
        clone.getReplicators().clear();
        Pair<String, Integer> master = currentMetaManager.getApplierMaster(clusterId, backupClusterId);
        if (master != null) {
            STATE_LOGGER.info("[getApplierMaster] for {}, {} is {}:{}", clusterId, backupClusterId, master.getKey(), master.getValue());
        } else {
            STATE_LOGGER.error("[getApplierMaster] null, do nothing for {}, {}", clusterId, backupClusterId);
            return clone;
        }
        if (master != null) {
            Replicator replicator = new Replicator();
            replicator.setMaster(true);
            replicator.setIp(master.getKey());
            replicator.setApplierPort(master.getValue());
            clone.getReplicators().add(replicator);
        }

        if (mysqlMaster != null) {
            setMySQL(clone, mysqlMaster);
        }

        return clone;
    }

    private void setMySQL(DbCluster clone, Endpoint mysqlMaster) {
        List<Db> dbList = clone.getDbs().getDbs();
        Db dbMaster = null;
        for (Db db : dbList) {
            if (db.getIp().equalsIgnoreCase(mysqlMaster.getHost()) && db.getPort() == mysqlMaster.getPort()) {
                dbMaster = db;
            }
        }
        if (dbMaster != null) {  //or do nothing
            dbMaster.setMaster(true);
            clone.getDbs().getDbs().clear();
            clone.getDbs().getDbs().add(dbMaster);
        }
    }

    private Endpoint getMySQLMaster(String clusterId) {
        Endpoint mysqlMaster = null;
        try {
            mysqlMaster = currentMetaManager.getMySQLMaster(clusterId);
            if (mysqlMaster != null) {
                logger.warn("[mysqlMaster] query from currentMetaManager {}", mysqlMaster);
            } else {
                logger.info("[mysqlMaster] query null from currentMetaManager, just OK and use master from dcCache");
            }
        } catch (Throwable t) {
        }
        return mysqlMaster;
    }

    public void setExecutors(ExecutorService executors) {
        this.executors = executors;
    }
}
