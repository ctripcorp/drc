package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.utils.NameUtils;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.RegionCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DcComparator;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author limingdong
 * @create 2020/4/20
 */
@Component
public class DefaultCurrentMetaManager extends AbstractLifecycleObservable implements CurrentMetaManager, Observer {

    private int slotCheckInterval = 60 * 1000;

    @Autowired
    private SlotManager slotManager;

    @Autowired
    private CurrentClusterServer currentClusterServer;

    @Autowired
    private RegionCache regionCache;

    private CurrentMeta currentMeta = new CurrentMeta();

    private Set<Integer> currentSlots = new HashSet<>();

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> slotCheckFuture;

    @Autowired
    private List<StateChangeHandler> stateHandlers;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    public DefaultCurrentMetaManager() {
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        setExecutors(executors);
        regionCache.addObserver(this);
        logger.info("[stateHandlers] has {}", stateHandlers);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        for(Integer slotId : currentClusterServer.slots()){
            addSlot(slotId);
        }


        slotCheckFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() {
                logger.info("[checkAddOrRemoveSlots] start");
                checkAddOrRemoveSlots();
            }
        }, slotCheckInterval, slotCheckInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void addObserver(Observer observer) {
        logger.info("[addObserver]{}", observer);
        super.addObserver(observer);
    }


    protected void checkAddOrRemoveSlots() {

        Set<Integer> slots = slotManager.getSlotsByServerId(currentClusterServer.getServerId(), false);

        Pair<Set<Integer>, Set<Integer>> result = getAddAndRemove(slots, currentSlots);

        for(Integer slotId : result.getKey()){
            addSlot(slotId);
        }

        for(Integer slotId : result.getValue()){
            deleteSlot(slotId);
        }
    }


    protected Pair<Set<Integer>, Set<Integer>> getAddAndRemove(Set<Integer> future, Set<Integer> current) {

        Set<Integer> added = new HashSet<>(future);
        added.removeAll(current);

        if(added.size() > 0){
            logger.info("[checkAddOrRemoveSlots][to add]{}", added);
        }

        Set<Integer> toRemove = new HashSet<>(current);
        toRemove.removeAll(future);

        if(toRemove.size() > 0){
            logger.info("[checkAddOrRemoveSlots][toRemove]{}", toRemove);
        }

        return new Pair<>(added, toRemove);
    }


    @Override
    protected void doStop() throws Exception {

        slotCheckFuture.cancel(true);
        super.doStop();
    }


    @Override
    protected void doDispose() throws Exception {

        currentMeta.release();
        super.doDispose();
    }

    private void handleClusterChanged(ClusterComparator clusterComparator) {

        String clusterId = clusterComparator.getCurrent().getId();
        if(currentMeta.hasCluster(clusterId)){

            currentMeta.changeCluster(clusterComparator);
            notifyObservers(clusterComparator);
        } else{
            logger.warn("[handleClusterChanged][but we do not has it]{}", clusterComparator);
            addCluster(clusterId);
        }
    }


    private void addCluster(String clusterId) {

        DbCluster clusterMeta = regionCache.getCluster(clusterId);

        logger.info("[addCluster]{}, {}", clusterId, clusterMeta);
        currentMeta.addCluster(clusterMeta);
        notifyObservers(new NodeAdded<DbCluster>(clusterMeta));
    }

    private void destroyCluster(DbCluster clusterMeta){
        removeCluster(clusterMeta);
    }

    private void removeCluster(DbCluster clusterMeta) {

        logger.info("[removeCluster]{}", clusterMeta.getId());
        boolean result = currentMeta.removeCluster(clusterMeta.getId()) != null;
        if(result){
            notifyObservers(new NodeDeleted<DbCluster>(clusterMeta));
        }
    }



    private void removeClusterInterested(String clusterId) {
        //notice, due to with empty replicator and applier, so just remove cache
        removeCluster(new DbCluster(clusterId));
    }

    @Override
    public Set<String> allClusters() {
        return new HashSet<>(currentMeta.allClusters());
    }

    @Override
    public void deleteSlot(int slotId) {

        currentSlots.remove(slotId);
        logger.info("[deleteSlot]{}", slotId);
        for(String clusterId : new HashSet<>(currentMeta.allClusters())){

            int currentSlotId = slotManager.getSlotIdByKey(clusterId);
            if(currentSlotId == slotId){
                removeClusterInterested(clusterId);
            }
        }
    }


    @Override
    public void addSlot(int slotId) {

        logger.info("[addSlot]{}", slotId);
        currentSlots.add(slotId);
        for(String clusterId : regionCache.getClusters()){

            int currentSlotId = slotManager.getSlotIdByKey(clusterId);
            if(currentSlotId == slotId){
                addCluster(clusterId);
            }
        }
    }

    @Override
    public void exportSlot(int slotId) {

        logger.info("[exportSlot]{}", slotId);
        deleteSlot(slotId);
    }

    @Override
    public void importSlot(int slotId) {

        logger.info("[importSlot][doNothing]{}", slotId);
    }

    @Override
    public Applier getActiveApplier(String clusterId, String backupRegistryKey) {
        return currentMeta.getActiveApplier(clusterId, backupRegistryKey);
    }

    @Override
    public List<Applier> getActiveAppliers(String clusterId, String backupClusterId) {
        return currentMeta.getActiveAppliers(clusterId, backupClusterId);
    }

    @Override
    public List<Applier> getActiveAppliers(String clusterId) {
        return currentMeta.getActiveAppliers(clusterId);
    }

    @Override
    public Messenger getActiveMessenger(String clusterId, String dbName) {
        return currentMeta.getActiveMessenger(clusterId, dbName);
    }

    @Override
    public List<Messenger> getActiveMessengers(String clusterId) {
        return currentMeta.getActiveMessengers(clusterId);
    }

    @Override
    public Replicator getActiveReplicator(String clusterId) {
        return currentMeta.getActiveReplicator(clusterId);
    }

    @Override
    public DbCluster getCluster(String clusterId) {
        return regionCache.getCluster(clusterId);
    }

    @Override
    public Route randomRoute(String clusterId, String dstDc) {
        return regionCache.randomRoute(clusterId, dstDc);
    }

    @Override
    public List<Replicator> getSurviveReplicators(String clusterId) {
        return currentMeta.getSurviveReplicators(clusterId);
    }

    @Override
    public Set<Instance> getAllMessengerInstances() {
        Set<Instance> set = new HashSet<>();
        for (String clusterId : regionCache.getClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            cluster.getMessengers().stream().map(e -> SimpleInstance.from(e.getIp(), e.getPort())).forEach(set::add);
        }
        return set;
    }

    @Override
    public Set<Instance> getAllApplierInstances() {
        Set<Instance> set = new HashSet<>();
        for (String clusterId : regionCache.getClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            cluster.getAppliers().stream().map(e -> SimpleInstance.from(e.getIp(), e.getPort())).forEach(set::add);
        }
        return set;
    }

    @Override
    public Set<Instance> getAllReplicatorInstances() {
        Set<Instance> set = new HashSet<>();
        for (String clusterId : regionCache.getClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            cluster.getReplicators().stream().map(e -> SimpleInstance.from(e.getIp(), e.getPort())).forEach(set::add);
        }
        return set;
    }

    @Override
    public Map<String, Map<String, List<Applier>>> getAllMetaAppliers() {
        Map<String, Map<String, List<Applier>>> map = new HashMap<>();
        for (String clusterId : allClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            List<Applier> applierList = MetaClone.cloneList(cluster.getAppliers());
            Map<String, List<Applier>> appliersGroupByBackupRegistryKey = applierList.stream().collect(Collectors.groupingBy(NameUtils::getApplierBackupRegisterKey));
            for (Map.Entry<String, List<Applier>> entry : appliersGroupByBackupRegistryKey.entrySet()) {
                String backupKey = entry.getKey();
                List<Applier> appliers = entry.getValue();
                Applier activeApplier = currentMeta.getActiveApplier(clusterId, backupKey);
                setMaster(appliers, activeApplier);
            }
            map.put(clusterId, appliersGroupByBackupRegistryKey);
        }
        return map;
    }

    @Override
    public Map<String, Map<String, List<Messenger>>> getAllMetaMessengers() {
        Map<String, Map<String, List<Messenger>>> map = new HashMap<>();
        for (String clusterId : allClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            List<Messenger> messengerList = MetaClone.cloneList(cluster.getMessengers());
            Map<String, List<Messenger>> messengersGroupByDbName = messengerList.stream().collect(Collectors.groupingBy(NameUtils::getMessengerDbName));
            for (Map.Entry<String, List<Messenger>> entry : messengersGroupByDbName.entrySet()) {
                String dbName = entry.getKey();
                List<Messenger> messengers = entry.getValue();
                Messenger activeMessenger = currentMeta.getActiveMessenger(clusterId, dbName);
                setMaster(messengers, activeMessenger);
            }
            map.put(clusterId, messengersGroupByDbName);
        }
        return map;
    }


    @Override
    public Map<String, List<Replicator>> getAllMetaReplicator() {
        Map<String, List<Replicator>> map = new HashMap<>();
        for (String clusterId : allClusters()) {
            DbCluster cluster = regionCache.getCluster(clusterId);
            List<Replicator> replicatorList = MetaClone.cloneList(cluster.getReplicators());
            Replicator activeReplicator = currentMeta.getActiveReplicator(clusterId);
            setMaster(replicatorList, activeReplicator);
            map.put(clusterId, replicatorList);
        }
        return map;
    }

    private void setMaster(List<? extends Instance> instances, Instance activeInstance) {
        if (activeInstance != null) {
            instances.forEach(e -> e.setMaster(activeInstance.equalsWithIpPort(e)));
        } else {
            instances.forEach(e -> e.setMaster(false));
        }
    }

    @Override
    public List<Applier> getSurviveAppliers(String clusterId, String backupRegistryKey) {
        return currentMeta.getSurviveAppliers(clusterId, backupRegistryKey);
    }

    @Override
    public List<Messenger> getSurviveMessengers(String clusterId, String dbName) {
        return currentMeta.getSurviveMessengers(clusterId, dbName);
    }

    @Override
    public String getCurrentMetaDesc() {

        Map<String, Object> desc = new HashMap<>();
        desc.put("meta", currentMeta);
        desc.put("currentSlots", currentSlots);
        JsonCodec codec = new JsonCodec(true, true);
        return codec.encode(desc);
    }

    @Override
    public void addResource(String registryKey, Releasable releasable) {
        currentMeta.addResource(registryKey, releasable);
    }

    @Override
    public void setSurviveReplicators(String registryKey, List<Replicator> surviveReplicators, Replicator activeReplicator) {
        currentMeta.setSurviveReplicators(registryKey, surviveReplicators, activeReplicator);
        notifyReplicatorActiveElected(registryKey, activeReplicator);
    }

    @Override
    public void setSurviveMessengers(String clusterId, String registryKey, List<Messenger> surviveMessengers, Messenger activeMessenger) {
        currentMeta.setSurviveMessengers(clusterId, registryKey, surviveMessengers, activeMessenger);
        notifyMessengerActiveElected(clusterId, activeMessenger);
    }

    @Override
    public void setSurviveAppliers(String clusterId, String registryKey,List<Applier> surviveAppliers, Applier activeApplier) {
        currentMeta.setSurviveAppliers(clusterId, registryKey, surviveAppliers, activeApplier);
        notifyApplierActiveElected(clusterId, activeApplier);
    }

    @Override
    public boolean watchReplicatorIfNotWatched(String registryKey) {
        return currentMeta.watchReplicatorIfNotWatched(registryKey);
    }

    @Override
    public boolean watchMessengerIfNotWatched(String registryKey) {
        return currentMeta.watchMessengerIfNotWatched(registryKey);
    }

    @Override
    public boolean watchApplierIfNotWatched(String registryKey) {
        return currentMeta.watchApplierIfNotWatched(registryKey);
    }

    @Override
    public void setApplierMaster(String clusterId, String backupClusterId, String ip, int port) {
        Pair<String, Integer> applierMaster = new Pair<String, Integer>(ip, port);
        if(currentMeta.setApplierMaster(clusterId, backupClusterId, applierMaster)){
            logger.info("[setApplierMaster]{},{},{}:{}", clusterId, backupClusterId, ip, port);
            notifyApplierMasterChanged(clusterId, backupClusterId, applierMaster);
        } else {
            logger.info("[setApplierMaster][applier master not changed!]{},{},{}:{}", clusterId, backupClusterId, ip, port);
        }
    }

    @Override
    public void setMySQLMaster(String clusterId, Endpoint endpoint) {
        currentMeta.setMySQLMaster(clusterId, endpoint);
    }

    @Override
    public void switchMySQLMaster(String clusterId, Endpoint endpoint) {
        currentMeta.setMySQLMaster(clusterId, endpoint);
        notifyMySQLMasterChanged(clusterId, endpoint);
    }

    @Override
    public Endpoint getMySQLMaster(String clusterId) {
        return currentMeta.getMySQLMaster(clusterId);
    }

    @Override
    public Pair<String, Integer> getApplierMaster(String clusterId, String backupClusterId) {
        return currentMeta.getApplierMaster(clusterId, backupClusterId);
    }

    @Override
    public void update(Object args, Observable observable) {
        if (args instanceof MetaRefreshDone) {
            return;
        }
        if(args instanceof DcComparator) {

            dcMetaChange((DcComparator) args);
        } else if(args instanceof DcRouteComparator) {

            routeChanges();
        } else {
            throw new IllegalArgumentException(String.format("unknown args(%s):%s", args.getClass(), args));
        }
    }

    @VisibleForTesting
    protected void dcMetaChange(DcComparator comparator) {

        for(DbCluster clusterMeta : comparator.getAdded()){
            if(currentClusterServer.hasKey(clusterMeta.getId())){
                addCluster(clusterMeta.getId());
            }else{
                logger.info("[dcMetaChange][add][not interested]{}", clusterMeta.getId());
            }
        }

        for(DbCluster clusterMeta : comparator.getRemoved()){
            if(currentClusterServer.hasKey(clusterMeta.getId())){
                destroyCluster(clusterMeta);
            }else{
                logger.info("[dcMetaChange][destroy][not interested]{}", clusterMeta.getId());
            }

        }

        for(@SuppressWarnings("rawtypes") MetaComparator changedComparator : comparator.getMofified()){
            ClusterComparator clusterComparator = (ClusterComparator) changedComparator;
            String clusterId = clusterComparator.getCurrent().getId();
            if(currentClusterServer.hasKey(clusterId)){
                handleClusterChanged(clusterComparator);
            }else{
                logger.info("[dcMetaChange][change][not interested]{}", clusterId);
            }
        }
    }

    @VisibleForTesting
    protected void routeChanges() {
        for(String clusterId : allClusters()) {
            DbCluster clusterMeta = regionCache.getCluster(clusterId);
            Map<String, Set<String>> upstreamDcClusterIds = getUpstreamDcClusterIds(clusterMeta);
            for (Map.Entry<String, Set<String>> upstreamDcClusterId : upstreamDcClusterIds.entrySet()) {
                String targetIdc = upstreamDcClusterId.getKey();
                Set<String> upstreamClusterIds = upstreamDcClusterId.getValue();
                if (randomRoute(clusterId, targetIdc) == null) {
                    continue;
                }
                for (String upstreamClusterId : upstreamClusterIds) {
                    refreshApplierMaster(clusterMeta, upstreamClusterId);
                }
            }
        }
    }

    @VisibleForTesting
    protected Map<String, Set<String>> getUpstreamDcClusterIds(DbCluster clusterMeta) {
        Map<String, Set<String>> upstreamDcClusterIdMap = Maps.newHashMap();
        List<Applier> applierList = clusterMeta.getAppliers();

        for (Applier applier : applierList) {
            String targetMhaName = applier.getTargetMhaName();
            String targetName = applier.getTargetName();
            String targetIdc = applier.getTargetIdc();
            if (StringUtils.isNotBlank(targetMhaName) && StringUtils.isNotBlank(targetIdc)) {
                String upstreamClusterId = RegistryKey.from(targetName, targetMhaName);
                if (upstreamDcClusterIdMap.containsKey(targetIdc)) {
                    upstreamDcClusterIdMap.get(targetIdc).add(upstreamClusterId);
                } else {
                    upstreamDcClusterIdMap.put(targetIdc, Sets.newHashSet(upstreamClusterId));
                }
            }
        }

        return upstreamDcClusterIdMap;
    }

    @VisibleForTesting
    protected void refreshApplierMaster(DbCluster clusterMeta, String upstreamClusterId) {
        String clusterId = clusterMeta.getId();
        notifyApplierMasterChanged(clusterId, upstreamClusterId, getApplierMaster(clusterId, upstreamClusterId));
    }

    private void notifyReplicatorActiveElected(String clusterId, Replicator activeReplicator) {

        for(StateChangeHandler stateHandler : stateHandlers){
            try {
                stateHandler.replicatorActiveElected(clusterId, activeReplicator);
            } catch (Exception e) {
                logger.error("[notifyReplicatorActiveElected]" + clusterId + "," + activeReplicator, e);
            }
        }
    }

    private void notifyMessengerActiveElected(String clusterId, Messenger activeMessenger) {

        for(StateChangeHandler stateHandler : stateHandlers){
            try {
                stateHandler.messengerActiveElected(clusterId, activeMessenger);
            } catch (Exception e) {
                logger.error("[notifyMessengerActiveElected]" + clusterId + "," + activeMessenger, e);
            }
        }
    }

    private void notifyApplierActiveElected(String clusterId, Applier activeApplier) {

        for(StateChangeHandler stateHandler : stateHandlers){
            try {
                stateHandler.applierActiveElected(clusterId, activeApplier);
            } catch (Exception e) {
                logger.error("[notifyApplierActiveElected]" + clusterId + "," + activeApplier, e);
            }
        }
    }

    private void notifyApplierMasterChanged(String clusterId, String backupClusterId, Pair<String, Integer> applierMaster) {
        for(StateChangeHandler stateHandler : stateHandlers){
            try {
                stateHandler.applierMasterChanged(clusterId, backupClusterId, applierMaster);
            } catch (Exception e) {
                logger.error("[notifyApplierMasterChanged]" + clusterId + "," + applierMaster, e);
            }
        }
    }

    private void notifyMySQLMasterChanged(String clusterId, Endpoint endpoint) {
        for(StateChangeHandler stateHandler : stateHandlers){
            try {
                stateHandler.mysqlMasterChanged(clusterId, endpoint);
            } catch (Exception e) {
                logger.error("[notifyMySQLMasterChanged]" + clusterId + "," + endpoint, e);
            }
        }
    }

    @Override
    public boolean hasCluster(String clusterId) {
        return currentMeta.hasCluster(clusterId);
    }

    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    public void setSlotCheckInterval(int slotCheckInterval) {
        this.slotCheckInterval = slotCheckInterval;
    }

    public void setRegionCache(RegionCache regionCache) {
        this.regionCache = regionCache;
    }

    @VisibleForTesting
    protected void addStateChangeHandler(StateChangeHandler handler) {
        if(stateHandlers == null) {
            stateHandlers = Lists.newArrayList();
        }
        stateHandlers.add(handler);
    }

    @VisibleForTesting
    protected void setCurrentMeta(CurrentMeta currentMeta) {
        this.currentMeta = currentMeta;
    }
}
