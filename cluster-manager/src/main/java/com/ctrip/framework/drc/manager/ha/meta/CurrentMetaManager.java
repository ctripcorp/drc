package com.ctrip.framework.drc.manager.ha.meta;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/20
 */
public interface CurrentMetaManager extends Observable {

    Set<String> allClusters();

    boolean hasCluster(String registryKey);

    void deleteSlot(int slotId);

    void addSlot(int slotId);

    void exportSlot(int slotId);

    void importSlot(int slotId);

    Applier getActiveApplier(String clusterId, String backupRegistryKey);

    List<Applier> getActiveAppliers(String clusterId, String backupClusterId);

    List<Applier> getActiveAppliers(String clusterId);

    Messenger getActiveMessenger(String clusterId, String dbName);

    List<Messenger> getActiveMessengers(String clusterId);

    Replicator getActiveReplicator(String clusterId);

    DbCluster getCluster(String clusterId);

    List<Replicator> getSurviveReplicators(String clusterId);

    List<Messenger> getSurviveMessengers(String clusterId, String dbName);

    List<Applier> getSurviveAppliers(String clusterId, String backupRegistryKey);

    String getCurrentMetaDesc();

    /************* update support *****************/

    void addResource(String registryKey, Releasable releasable);

    void setSurviveReplicators(String registryKey, List<Replicator> surviveReplicators, Replicator activeReplicator);

    void setSurviveAppliers(String clusterId, List<Applier> surviveAppliers, Applier activeApplier);

    void setSurviveMessengers(String registryKey, List<Messenger> surviveMessengers, Messenger activeMessenger);

    boolean watchReplicatorIfNotWatched(String registryKey);

    boolean watchApplierIfNotWatched(String registryKey);

    boolean watchMessengerIfNotWatched(String registryKey);

    void setApplierMaster(String registryKey, String backupClusterId, String ip, int port);

    void setMySQLMaster(String registryKey, Endpoint endpoint);

    void switchMySQLMaster(String registryKey, Endpoint endpoint);

    Endpoint getMySQLMaster(String registryKey);

    Pair<String, Integer> getApplierMaster(String clusterId, String backupRegistryKey);

    Route randomRoute(String clusterId, String dstDc);

}
