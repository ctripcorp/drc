package com.ctrip.framework.drc.manager.ha.cluster.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.http.AsyncHttpClientFactory;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.healthcheck.inquirer.ApplierInfoInquirer;
import com.ctrip.framework.drc.manager.healthcheck.inquirer.ReplicatorInfoInquirer;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ApplierNotifier;
import com.ctrip.framework.drc.manager.healthcheck.notifier.MessengerNotifier;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ReplicatorNotifier;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

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
    
    @Override
    protected void doStop() throws Exception {
        super.doStop();
        AsyncHttpClientFactory.closeAll();
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
        String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
        STATE_LOGGER.info("[registerApplier] for {},{}", registryKey, body);
        applierNotifier.notifyRegister(registryKey, body);
        return body;
    }

    public Pair<List<String>, List<ApplierInfoDto>> getApplierInfoInner(List<? extends Instance> appliers) {
        List<Pair<String, Integer>> applierIpAndPorts = appliers.stream().map(applier -> Pair.from(applier.getIp(), applier.getPort())).distinct().collect(Collectors.toList());
        ApplierInfoInquirer infoInquirer = ApplierInfoInquirer.getInstance();
        List<Future<List<ApplierInfoDto>>> futures = Lists.newArrayList();
        for (Pair<String, Integer> pair : applierIpAndPorts) {
            futures.add(infoInquirer.query(pair.getKey() + ":" + pair.getValue()));
        }
        List<ApplierInfoDto> applierInfoDtos = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList();
        for (int i = 0; i < futures.size(); i++) {
            Future<List<ApplierInfoDto>> future = futures.get(i);
            Pair<String, Integer> pair = applierIpAndPorts.get(i);
            String ip = pair.getKey();
            Integer port = pair.getValue();
            try {
                List<ApplierInfoDto> infoDtos = future.get(1000, TimeUnit.MILLISECONDS);
                infoDtos.forEach(e -> {
                    e.setIp(ip);
                    e.setPort(port);
                });
                applierInfoDtos.addAll(infoDtos);
                validIps.add(ip);
                EventMonitor.DEFAULT.logEvent("drc.cm.inquiry.applier.success", ip + ":" + port);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                future.cancel(true);
                QUERY_INFO_LOGGER.error("get applier fail, skip for: {}", pair.getKey() + ":" + pair.getValue());
                EventMonitor.DEFAULT.logEvent("drc.cm.inquiry.applier.fail", ip + ":" + port);
            }
        }
        return Pair.from(validIps, applierInfoDtos);
    }

    @Override
    public Pair<List<String>, List<ApplierInfoDto>> getMessengerInfo(List<? extends Instance> messengers) {
        Pair<List<String>, List<ApplierInfoDto>> applierInfoInner = getApplierInfoInner(messengers);
        applierInfoInner.getValue().removeIf(e -> !e.getRegistryKey().contains(DRC_MQ));
        return applierInfoInner;
    }

    @Override
    public Pair<List<String>, List<ApplierInfoDto>> getApplierInfo(List<? extends Instance> appliers) {
        Pair<List<String>, List<ApplierInfoDto>> applierInfoInner = getApplierInfoInner(appliers);
        applierInfoInner.getValue().removeIf(e -> e.getRegistryKey().contains(DRC_MQ));
        return applierInfoInner;
    }

    @Override
    public Pair<List<String>, List<ReplicatorInfoDto>> getReplicatorInfo(List<? extends Instance> replicators) {
        List<Pair<String, Integer>> applierIpAndPorts = replicators.stream().map(applier -> Pair.from(applier.getIp(), applier.getPort())).distinct().collect(Collectors.toList());
        ReplicatorInfoInquirer infoInquirer = ReplicatorInfoInquirer.getInstance();
        List<Future<List<ReplicatorInfoDto>>> futures = Lists.newArrayList();
        for (Pair<String, Integer> pair : applierIpAndPorts) {
            futures.add(infoInquirer.query(pair.getKey() + ":" + pair.getValue()));
        }
        List<ReplicatorInfoDto> replicatorInfoDtos = Lists.newArrayList();
        List<String> validIps = Lists.newArrayList();
        for (int i = 0; i < futures.size(); i++) {
            Future<List<ReplicatorInfoDto>> future = futures.get(i);
            Pair<String, Integer> pair = applierIpAndPorts.get(i);
            String ip = pair.getKey();
            Integer port = pair.getValue();
            try {
                List<ReplicatorInfoDto> infoDtos = future.get(1000, TimeUnit.MILLISECONDS);
                infoDtos.forEach(e -> {
                    e.setIp(ip);
                    e.setPort(port);
                });
                replicatorInfoDtos.addAll(infoDtos);
                validIps.add(ip);
                EventMonitor.DEFAULT.logEvent("drc.cm.inquiry.replicator.success", ip + ":" + port);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                QUERY_INFO_LOGGER.error("get replicator fail, skip for: {}", pair.getKey() + ":" + pair.getValue());
                EventMonitor.DEFAULT.logEvent("drc.cm.inquiry.replicator.fail", ip + ":" + port);
            }
        }
        return Pair.from(validIps, replicatorInfoDtos);
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
        String registryKey = NameUtils.getMessengerRegisterKey(clusterId, messenger);
        STATE_LOGGER.info("[registerMessenger] for {},{}", registryKey, body);
        messengerNotifier.notifyRegister(registryKey, body);
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
        String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
        STATE_LOGGER.info("[addApplier] for {},{}", registryKey, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][addApplier] replicators and do nothing for {}", registryKey);
            return body;
        }
        applierNotifier.notifyAdd(registryKey, body);

        String backupRegistryKey = NameUtils.getApplierBackupRegisterKey(applier);
        List<Applier> appliers = currentMetaManager.getSurviveAppliers(clusterId, backupRegistryKey);
        for (Applier a : appliers) {
            if (!a.equalsWithIpPort(applier) && a.getTargetMhaName().equalsIgnoreCase(applier.getTargetMhaName())
                    && ObjectUtils.equals(a.getIncludedDbs(), applier.getIncludedDbs())) {
                removeApplier(clusterId, a, false);
            }
        }
        return body;
    }

    @Override
    public DbCluster addMessenger(String clusterId, Messenger messenger) {
        MessengerNotifier messengerNotifier = MessengerNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshMessenger(clusterId, messenger);
        String registryKey = NameUtils.getMessengerRegisterKey(clusterId, messenger);
        STATE_LOGGER.info("[addMessenger] for {},{}", registryKey, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty] replicators and do nothing for {}", registryKey);
            return body;
        }
        messengerNotifier.notifyAdd(registryKey, body);

        String dbName = NameUtils.getMessengerDbName(messenger);
        List<Messenger> messengers = currentMetaManager.getSurviveMessengers(clusterId, dbName);
        for (Messenger m : messengers) {
            if (!m.equalsWithIpPort(messenger) && ObjectUtils.equals(m.getIncludedDbs(), messenger.getIncludedDbs())) {
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
        String registryKey = NameUtils.getMessengerRegisterKey(clusterId, messenger);
        STATE_LOGGER.info("[removeMessenger] for {},{}, delete: {}", registryKey, messenger, delete);
        messengerNotifier.notifyRemove(registryKey, messenger, delete);
    }

    @Override
    public void removeApplier(String clusterId, Applier applier, boolean delete) {
        if (config.getMigrationBlackIps().contains(applier.getIp())) {
            logger.info("[skipRemove] black ips are: {}, applier ip is:{}", config.getMigrationBlackIps(), applier.getIp());
            return;
        }
        ApplierNotifier applierNotifier = ApplierNotifier.getInstance();
        String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
        STATE_LOGGER.info("[removeApplier] for {},{}", registryKey, applier);
        applierNotifier.notifyRemove(registryKey, applier, delete);
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
        String newClusterId = NameUtils.getApplierRegisterKey(clusterId, applier);
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
        String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
        STATE_LOGGER.info("[applierPropertyChange] for {},{}", registryKey, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][applierPropertyChange] replicators and do nothing for {}", clusterId);
            return body;
        }
        applierNotifier.notifyAdd(registryKey, body);
        return body;
    }

    @Override
    public DbCluster messengerPropertyChange(String clusterId, Messenger messenger) {
        MessengerNotifier messengerNotifier = MessengerNotifier.getInstance();
        DbCluster body = getDbClusterWithRefreshMessenger(clusterId, messenger);
        String registryKey = NameUtils.getMessengerRegisterKey(clusterId, messenger);
        STATE_LOGGER.info("[messengerPropertyChange] for {},{}", registryKey, body);
        List<Replicator> replicators = body.getReplicators();
        if (replicators == null || replicators.isEmpty()) {
            STATE_LOGGER.warn("[Empty][messengerPropertyChange] replicators and do nothing for {}", clusterId);
            return body;
        }
        messengerNotifier.notifyAdd(registryKey, body);
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

            String registryKey = NameUtils.getApplierRegisterKey(clusterId, applier);
            applierNotifier.notify(registryKey, dbCluster);
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
