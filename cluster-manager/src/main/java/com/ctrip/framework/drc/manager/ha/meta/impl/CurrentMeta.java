package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class CurrentMeta implements Releasable {

    @JsonIgnore
    private Logger logger = LoggerFactory.getLogger(CurrentMeta.class);

    private Map<String, CurrentClusterMeta> currentMetas = new ConcurrentHashMap<>();

    public Set<String> allClusters() {
        return new HashSet<>(currentMetas.keySet());
    }

    public boolean hasCluster(String clusterId) {
        return currentMetas.get(clusterId) != null;
    }

    public boolean setSurviveReplicators(String clusterId, List<Replicator> surviveReplicators, Replicator activeReplicator) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.setSurviveReplicators(surviveReplicators, activeReplicator);
    }

    public void setSurviveMessengers(String clusterId, List<Messenger> surviveMessengers, Messenger activeMessenger) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        currentShardMeta.setSurviveMessengers(surviveMessengers, activeMessenger);
    }

    public void setSurviveAppliers(String clusterId, List<Applier> surviveAppliers, Applier activeApplier) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        currentShardMeta.setSurviveAppliers(surviveAppliers, activeApplier);

    }

    public boolean watchReplicatorIfNotWatched(String registryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(registryKey);
        return currentShardMeta.watchReplicatorIfNotWatched();
    }

    public boolean watchMessengerIfNotWatched(String registryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(registryKey);
        return currentShardMeta.watchMessengerIfNotWatched(registryKey);
    }

    public boolean watchApplierIfNotWatched(String registryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(registryKey);
        return currentShardMeta.watchApplierIfNotWatched(registryKey);
    }

    public void addResource(String registryKey, Releasable releasable) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(registryKey);
        currentShardMeta.addResource(releasable);

    }

    public List<Replicator> getSurviveReplicators(String clusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getSurviveReplicators();
    }

    public List<Messenger> getSurviveMessengers(String clusterId, String dbName) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getSurviveMessengers(dbName);
    }

    public List<Applier> getSurviveAppliers(String clusterId, String backupRegistryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getSurviveAppliers(backupRegistryKey);
    }

    public boolean setApplierMaster(String clusterId, String backupClusterId, Pair<String, Integer> replicatorMaster) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.setApplierMaster(backupClusterId, replicatorMaster);
    }

    public void setMySQLMaster(String clusterId, Endpoint endpoint) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        currentShardMeta.setMySQLMaster(endpoint);
    }

    public Endpoint getMySQLMaster(String clusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getMySQLMaster();
    }

    public Pair<String, Integer> getApplierMaster(String clusterId, String backupClusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getApplierMaster(backupClusterId);
    }

    public Applier getActiveApplier(String clusterId, String backupRegistryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveApplier(backupRegistryKey);
    }

    public List<Applier> getActiveAppliers(String clusterId, String backupClusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveAppliers(backupClusterId);
    }

    public List<Applier> getActiveAppliers(String clusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveAppliers();
    }

    public Messenger getActiveMessenger(String clusterId, String dbName) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveMessenger(dbName);
    }

    public List<Messenger> getActiveMessengers(String clusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveMessenger();
    }

    public Replicator getActiveReplicator(String clusterId) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMetaOrThrowException(clusterId);
        return currentShardMeta.getActiveReplicator();
    }

    private CurrentClusterMeta getCurrentClusterMetaOrThrowException(String registryKey) {
        CurrentClusterMeta currentShardMeta = getCurrentClusterMeta(registryKey);
        if (currentShardMeta == null) {
            throw new IllegalArgumentException("can not find :" + registryKey);
        }
        return currentShardMeta;
    }

    private CurrentClusterMeta getCurrentClusterMeta(String registryKey) {
        String clusterId = RegistryKey.from(registryKey).toString();
        return currentMetas.get(clusterId);
    }

    @Override
    public void release() throws Exception {

        for (CurrentClusterMeta currentClusterMeta : currentMetas.values()) {
            try {
                currentClusterMeta.release();
            } catch (Exception e) {
                logger.error("[release] {}", currentClusterMeta.getClusterId(), e);
            }
        }
    }

    public void addCluster(final DbCluster clusterMeta) {
        MapUtils.getOrCreate(currentMetas, clusterMeta.getId(),
                () -> {
                    logger.info("[addCluster][create]{}", clusterMeta.getId());
                    Dbs dbs = clusterMeta.getDbs();
                    List<Db> dbList = dbs.getDbs();
                    Endpoint endpoint = null;
                    for (Db db : dbList) {
                        if (db.isMaster()) {
                            endpoint = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                        }
                    }
                    return new CurrentClusterMeta(clusterMeta.getId(), endpoint);
                });

    }

    public CurrentClusterMeta removeCluster(String clusterId) {
        CurrentClusterMeta currentClusterMeta = currentMetas.remove(clusterId);
        try {
            currentClusterMeta.release();
        } catch (Exception e) {
            logger.error("[remove]" + clusterId, e);
        }
        return currentClusterMeta;
    }

    public void changeCluster(ClusterComparator comparator) {

        String clusterId = comparator.getCurrent().getId();
        final CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterId);
        if (currentClusterMeta == null) {
            throw new IllegalArgumentException("cluster not exist:" + comparator);
        }

    }

    public static class CurrentClusterMeta implements Releasable {

        @JsonIgnore
        private Logger logger = LoggerFactory.getLogger(getClass());

        @JsonIgnore
        private List<Releasable> resources = Lists.newArrayList();

        private AtomicBoolean replicatorWatched = new AtomicBoolean(false);

        private String clusterId;

        private List<Replicator> surviveReplicators = Lists.newArrayList();  // all replicators

        private Endpoint mysqlMaster;  //mysql master

        // key(backupRegistryKey): targetName.targetMha[.dstDB]
        private Map<String, List<Applier>> surviveAppliers = Maps.newConcurrentMap();  // all appliers

        // key: dbName（default name is _drc_mq）
        private Map<String, List<Messenger>> surviveMessengers = Maps.newConcurrentMap(); // all messengers

        // key(backupClusterId): targetName.targetMha
        private Map<String, Pair<String, Integer>> applierMasters = Maps.newConcurrentMap();  // another zone replicator connected by applier

        // key(registryKey): name.mha.dstMha[.dstDB]
        private Map<String, AtomicBoolean> appliersWatched = Maps.newConcurrentMap();

        // key(registryKey): name.mha._drc_mq[.dstDB]
        private Map<String, AtomicBoolean> messengersWatched = Maps.newConcurrentMap();

        public CurrentClusterMeta() {

        }

        public void addResource(Releasable releasable) {
            resources.add(releasable);
        }

        @Override
        public void release() {
            logger.info("[release]{}", clusterId);
            for (Releasable resource : resources) {
                try {
                    resource.release();
                } catch (Exception e) {
                    logger.error("[release]" + resource, e);
                }
            }
        }

        public boolean watchReplicatorIfNotWatched() {
            return replicatorWatched.compareAndSet(false, true);
        }

        public boolean watchApplierIfNotWatched(String registryKey) {
            AtomicBoolean applierWatched = appliersWatched.get(registryKey);
            if (applierWatched == null) {
                applierWatched = new AtomicBoolean(false);
                appliersWatched.put(registryKey, applierWatched);
            }
            return applierWatched.compareAndSet(false, true);
        }

        public boolean watchMessengerIfNotWatched(String registryKey) {
            AtomicBoolean messengerWatched = messengersWatched.get(registryKey);
            if (messengerWatched == null) {
                messengerWatched = new AtomicBoolean(false);
                messengersWatched.put(registryKey, messengerWatched);
            }
            return messengerWatched.compareAndSet(false, true);
        }

        public CurrentClusterMeta(String clusterId, Endpoint endpoint) {
            this.clusterId = clusterId;
            setMySQLMaster(endpoint);
        }

        @SuppressWarnings("unchecked")
        public boolean setSurviveReplicators(List<Replicator> surviveReplicators, Replicator activeReplicator) {
            if (surviveReplicators.size() > 0) {
                if (!checkIn(surviveReplicators, activeReplicator)) {
                    throw new IllegalArgumentException("active not in all survivors " + activeReplicator + ", all:" + this.surviveReplicators);
                }
                this.surviveReplicators = (List<Replicator>) MetaClone.clone((Serializable) surviveReplicators);
                logger.info("[setSurviveReplicators]{},{},{}", clusterId, surviveReplicators, activeReplicator);
                return doSetActive(clusterId, activeReplicator, this.surviveReplicators);
            } else {
                logger.info("[setSurviveReplicators][survive replicator none, clear]{},{},{}", clusterId, surviveReplicators, activeReplicator);
                this.surviveReplicators.clear();
                return false;
            }
        }

        public void setSurviveMessengers(List<Messenger> surviveMessengers, Messenger activeMessenger) {
            if (surviveMessengers.size() > 0) {
                if (!checkIn(surviveMessengers, activeMessenger)) {
                    throw new IllegalArgumentException("active not in all survivors " + activeMessenger + ", all:" + this.surviveMessengers);
                }

                String dbName = NameUtils.getMessengerDbName(activeMessenger);
                List<Messenger> messengers = (List<Messenger>) MetaClone.clone((Serializable) surviveMessengers);
                this.surviveMessengers.put(dbName, messengers);
                logger.info("[setSurviveMessengers]{},{},{},{}", clusterId, dbName, surviveMessengers, activeMessenger);
                doSetActive(dbName, activeMessenger, messengers);
            } else {
                logger.info("[setSurviveMessengers][survive messenger none, clear]{},{},{}", clusterId, surviveMessengers, activeMessenger);
                //TODO: clear too much
                this.surviveMessengers.clear();
            }
        }

        public void setSurviveAppliers(List<Applier> surviveAppliers, Applier activeApplier) {
            if (surviveAppliers.size() > 0) {
                if (!checkIn(surviveAppliers, activeApplier)) {
                    throw new IllegalArgumentException("active not in all survivors " + activeApplier + ", all:" + this.surviveAppliers);
                }
                String backupRegistryKey = NameUtils.getApplierBackupRegisterKey(activeApplier);
                List<Applier> appliers = (List<Applier>) MetaClone.clone((Serializable) surviveAppliers);
                this.surviveAppliers.put(backupRegistryKey, appliers);
                logger.info("[setSurviveAppliers]{},{},{},{}", clusterId, backupRegistryKey, surviveAppliers, activeApplier);
                doSetActive(backupRegistryKey, activeApplier, appliers);
            } else {
                logger.info("[setSurviveAppliers][survive applier none, clear]{},{},{}", clusterId, surviveAppliers, activeApplier);
                //TODO: clear too much
                this.surviveAppliers.clear();
            }
        }

        public boolean setApplierMaster(String backupClusterId, Pair<String, Integer> applierMaster) {
            logger.info("[setApplierMaster]{},{}", backupClusterId, applierMaster);
            Pair<String, Integer> previousApplierMaster = applierMasters.get(backupClusterId);

            if (ObjectUtils.equals(previousApplierMaster, applierMaster)) {
                return false;
            }

            if (applierMaster == null) {
                applierMasters.remove(backupClusterId);
            } else {
                applierMasters.put(backupClusterId, new Pair<String, Integer>(applierMaster.getKey(), applierMaster.getValue()));
            }

            return true;

        }

        public void setMySQLMaster(Endpoint master) {
            if (master != null) {
                logger.info("[mysqlMaster] switch from {} to {}", this.mysqlMaster, master);
                Endpoint endpoint = new DefaultEndPoint(master.getHost(), master.getPort(), master.getUser(), master.getPassword());
                this.mysqlMaster = endpoint;
            }
        }

        public Endpoint getMySQLMaster() {
            if (this.mysqlMaster == null) {
                return null;
            }
            return new DefaultEndPoint(mysqlMaster.getHost(), mysqlMaster.getPort(), mysqlMaster.getUser(), mysqlMaster.getPassword());
        }

        public Pair<String, Integer> getApplierMaster(String backupClusterId) {
            backupClusterId = RegistryKey.from(backupClusterId).toString();
            Pair<String, Integer> res = applierMasters.get(backupClusterId);
            if (res == null) {
                return null;
            }
            return new Pair<String, Integer>(res.getKey(), res.getValue());
        }

        public Applier getActiveApplier(String backupRegistryKey) {  //single idc
            List<Applier> appliers = surviveAppliers.get(backupRegistryKey);
            if (appliers != null) {
                for (Applier survive : appliers) {
                    if (survive.isMaster()) {
                        return survive;
                    }
                }
            }
            return null;
        }

        public List<Applier> getActiveAppliers() {  //all idc
            List<Applier> appliers = Lists.newArrayList();
            for (Map.Entry<String, List<Applier>> entry : surviveAppliers.entrySet()) {
                for (Applier survive : entry.getValue()) {
                    if (survive.isMaster()) {
                        appliers.add(survive);
                    }
                }
            }
            return appliers;
        }

        public List<Applier> getActiveAppliers(String backupClusterId) {
            List<Applier> appliers = Lists.newArrayList();
            for (Map.Entry<String, List<Applier>> entry : surviveAppliers.entrySet()) {
                for (Applier survive : entry.getValue()) {
                    if (survive.isMaster()
                            && RegistryKey.from(survive.getTargetName(), survive.getTargetMhaName()).equalsIgnoreCase(backupClusterId)) {
                        appliers.add(survive);
                    }
                }
            }
            return appliers;
        }

        public Messenger getActiveMessenger(String dbName) {
            List<Messenger> messengers = surviveMessengers.get(dbName);
            if (messengers != null) {
                for (Messenger survive : messengers) {
                    if (survive.isMaster()) {
                        return survive;
                    }
                }
            }
            return null;
        }

        public List<Messenger> getActiveMessenger() {
            List<Messenger> messengers = Lists.newArrayList();
            for (Map.Entry<String, List<Messenger>> entry : surviveMessengers.entrySet()) {
                for (Messenger survive : entry.getValue()) {
                    if (survive.isMaster()) {
                        messengers.add(survive);
                    }
                }
            }
            return messengers;
        }

        public Replicator getActiveReplicator() {
            for (Replicator survive : surviveReplicators) {
                if (survive.isMaster()) {
                    return survive;
                }
            }
            return null;
        }

        public <T extends Instance> boolean doSetActive(String backupClusterId, T activeInstance, List<T> activeInstances) {

            boolean changed = false;
            logger.info("[doSetActive]{}:{},{}", clusterId, backupClusterId, activeInstance);
            for (T survive : activeInstances) {

                if (survive.equalsWithIpPort(activeInstance)) {
                    if (!survive.getMaster()) {
                        survive.setMaster(true);
                        changed = true;
                    }
                } else {
                    if (survive.getMaster()) {
                        survive.setMaster(false);
                    }
                }
            }
            return changed;
        }

        private <T extends Instance> boolean checkIn(List<T> surviveInstances, T activeInstance) {
            for (T survive : surviveInstances) {
                if (survive.equalsWithIpPort(activeInstance)) {
                    return true;
                }
            }
            return false;
        }

        public List<Replicator> getSurviveReplicators() {
            return (List<Replicator>) MetaClone.clone((Serializable) surviveReplicators);
        }

        public List<Messenger> getSurviveMessengers(String dbName) {
            List<Messenger> messengers = surviveMessengers.get(dbName);
            if (messengers == null) {
                return null;
            }
            return (List<Messenger>) MetaClone.clone((Serializable) messengers);
        }

        public List<Applier> getSurviveAppliers(String backupRegistryKey) {
            List<Applier> appliers = surviveAppliers.get(backupRegistryKey);
            if (appliers == null) {
                return null;
            }
            return (List<Applier>) MetaClone.clone((Serializable) appliers);
        }

        public String getClusterId() {
            return clusterId;
        }

        public DbCluster getDbCluster() {
            DbCluster dbCluster = new DbCluster(clusterId);
            getSurviveReplicators().stream().forEach(replicator -> dbCluster.addReplicator(replicator));
            return dbCluster;
        }
    }

    @Override
    public String toString() {
        JsonCodec codec = new JsonCodec(true, true);
        return codec.encode(this);
    }

    public static CurrentMeta fromJson(String json) {
        JsonCodec jsonCodec = new JsonCodec(true, true);
        return jsonCodec.decode(json, CurrentMeta.class);
    }

}
