package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ApplierNotifier;
import com.ctrip.framework.drc.manager.healthcheck.notifier.MessengerNotifier;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ReplicatorNotifier;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MQ;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.STATE_LOGGER;

/**
 * dbcluster combined with copy from dcMetaCache and currentMetaManager
 * @Author limingdong
 * @create 2020/5/6
 */
@Component
public class DefaultInstanceStateController extends AbstractLifecycle implements InstanceStateController, TopElement {

    @Autowired
    private RegionCache regionMetaCache;

    @Autowired
    private CurrentMetaManager currentMetaManager;

    @Autowired
    private ClusterManagerConfig config;

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
        STATE_LOGGER.info("[addReplicator] for {},{}", clusterId, body);
        replicatorNotifier.notifyAdd(clusterId, body);
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
        STATE_LOGGER.info("[registerReplicator] for {},{}", clusterId, body);
        replicatorNotifier.notifyRegister(clusterId, body);
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
        String targetMhaName = applier.getTargetMhaName();
        String newClusterId = RegistryKey.from(clusterId, targetMhaName);
        STATE_LOGGER.info("[registerApplier] for {},{}", newClusterId, body);
        applierNotifier.notifyRegister(newClusterId, body);
        return body;
    }

    /**
     * notify messenger to register zookeeper
     * @param clusterId
     * @param messenger
     * @return
     */
    @Override
    public DbCluster registerMessenger(String clusterId, Messenger messenger) {
        MessengerNotifier messengerNotifier = MessengerNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshMessenger(clusterId, messenger);
        STATE_LOGGER.info("[registerMessenger] for {},{}", clusterId, body);
        messengerNotifier.notifyRegister(clusterId, body);
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
        String targetMhaName = applier.getTargetMhaName();
        String newClusterId = RegistryKey.from(clusterId, targetMhaName);
        STATE_LOGGER.info("[addApplier] for {},{}", newClusterId, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][addApplier] replicators and do nothing for {}", clusterId);
            return body;
        }
        applierNotifier.notifyAdd(newClusterId, body);

        List<Applier> appliers = currentMetaManager.getSurviveAppliers(clusterId, RegistryKey.from(applier.getTargetName(), applier.getTargetMhaName()));
        for (Applier a : appliers) {
            if (!a.equalsWithIpPort(applier) && a.getTargetMhaName().equalsIgnoreCase(applier.getTargetMhaName())) {
                removeApplier(clusterId, a, false);
            }
        }
        return body;
    }

    @Override
    public DbCluster addMessenger(String clusterId, Messenger messenger) {
        MessengerNotifier messengerNotifier = MessengerNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshMessenger(clusterId, messenger);
        STATE_LOGGER.info("[addMessenger] for {},{}", clusterId, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty] replicators and do nothing for {}", clusterId);
            return body;
        }
        messengerNotifier.notifyAdd(clusterId, body);

        List<Messenger> messengers = currentMetaManager.getSurviveMessengers(clusterId);
        for (Messenger m : messengers) {
            if (!m.equalsWithIpPort(messenger)) {
                removeMessenger(clusterId, m, false);
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
        replicatorNotifier.notifyRemove(clusterId, replicator, true);
    }

    @Override
    public void removeMessenger(String clusterId, Messenger messenger, boolean delete) {
        if (config.getMigrationBlackIps().contains(messenger.getIp())) {
            logger.info("[skipRemove] black ips are: {}, messenger ip is:{}", config.getMigrationBlackIps(), messenger.getIp());
            return;
        }
        MessengerNotifier messengerNotifier = MessengerNotifier.getInstance();
        String newClusterId = RegistryKey.from(clusterId, DRC_MQ);
        STATE_LOGGER.info("[removeMessenger] for {},{}, delete: {}", newClusterId, messenger, delete);
        messengerNotifier.notifyRemove(newClusterId, messenger, delete);
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
        applierNotifier.notifyRemove(newClusterId, applier, delete);
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
        String targetMhaName = applier.getTargetMhaName();
        String newClusterId = RegistryKey.from(clusterId, targetMhaName);
        STATE_LOGGER.info("[applierMasterChange] for {},{}", newClusterId, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][applierMasterChange] replicators and do nothing for {}", clusterId);
            return body;
        }
        applierNotifier.notifyAdd(newClusterId, body);
        return body;
    }

    @Override
    public DbCluster applierPropertyChange(String clusterId, Applier applier) {
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshApplier(clusterId, applier);
        String targetMhaName = applier.getTargetMhaName();
        String newClusterId = RegistryKey.from(clusterId, targetMhaName);
        STATE_LOGGER.info("[applierPropertyChange] for {},{}", newClusterId, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][applierPropertyChange] replicators and do nothing for {}", clusterId);
            return body;
        }
        applierNotifier.notifyAdd(newClusterId, body);
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
            List<Replicator> replicators = dbCluster.getReplicators();
            if (replicators == null || replicators.isEmpty()) {
                STATE_LOGGER.warn("[Empty][mysqlMasterChanged] replicators and do nothing for {}", clusterId);
                continue;
            }
            String targetMhaName = applier.getTargetMhaName();
            String newClusterId = RegistryKey.from(clusterId, targetMhaName);
            applierNotifier.notify(newClusterId, dbCluster);
            res.add(dbCluster);
        }

        ReplicatorNotifier replicatorNotifier = ReplicatorNotifier.getInstance();
        DbCluster dbCluster = getDbClusterWithRefreshReplicator(clusterId, replicator, mysqlMaster);
        dbCluster.getReplicators().clear();
        dbCluster.getReplicators().add(replicator);
        replicatorNotifier.notify(clusterId, dbCluster);
        res.add(dbCluster);
        STATE_LOGGER.info("[mysqlMasterChanged] for {},{}", clusterId, res);

        return res;
    }

    private DbCluster getDbClusterWithRefreshReplicator(String clusterId, Replicator replicator) {
        return getDbClusterWithRefreshReplicator(clusterId, replicator, getMySQLMaster(clusterId));
    }

    private DbCluster getDbClusterWithRefreshReplicator(String clusterId, Replicator replicator, Endpoint mysqlMaster) {
        DbCluster dbCluster = regionMetaCache.getCluster(clusterId);
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
        DbCluster dbCluster = regionMetaCache.getCluster(clusterId);
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

    private DbCluster getDbClusterWithRefreshMessenger(String clusterId, Messenger messenger) {
        DbCluster dbCluster = regionMetaCache.getCluster(clusterId);
        DbCluster clone = MetaClone.clone(dbCluster);
        clone.getMessengers().clear();
        clone.getMessengers().add(messenger);

        //add local dc replicator
        clone.getReplicators().clear();
        Replicator master = currentMetaManager.getActiveReplicator(clusterId);
        if (master != null) {
            STATE_LOGGER.info("[getMessengerMaster] for {} is {}:{}", clusterId, master.getIp(), master.getPort());
        } else {
            STATE_LOGGER.error("[getMessengerMaster] null, do nothing for {}", clusterId);
            return clone;
        }

        Replicator replicator = new Replicator();
        replicator.setMaster(true);
        replicator.setIp(master.getIp());
        replicator.setApplierPort(master.getApplierPort());
        clone.getReplicators().add(replicator);

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
}
