package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.ha.StateChangeHandler;
import com.ctrip.framework.drc.manager.ha.cluster.CurrentClusterServer;
import com.ctrip.framework.drc.manager.ha.cluster.SlotManager;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DcComparator;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
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
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    private DcCache dcCache;

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

        dcCache.addObserver(this);
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

        DbCluster clusterMeta = dcCache.getCluster(clusterId);

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
        for(String clusterId : dcCache.getClusters()){

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
    public Applier getActiveApplier(String clusterId, String backupClusterId) {
        return currentMeta.getActiveApplier(clusterId, backupClusterId);
    }

    @Override
    public List<Applier> getActiveAppliers(String clusterId) {
        return currentMeta.getActiveAppliers(clusterId);
    }

    @Override
    public Replicator getActiveReplicator(String clusterId) {
        return currentMeta.getActiveReplicator(clusterId);
    }

    @Override
    public DbCluster getCluster(String clusterId) {
        return dcCache.getCluster(clusterId);
    }

    @Override
    public Route randomRoute(String clusterId, String dstDc) {
        return dcCache.randomRoute(clusterId, dstDc);
    }

    @Override
    public List<Replicator> getSurviveReplicators(String clusterId) {
        return currentMeta.getSurviveReplicators(clusterId);
    }

    @Override
    public List<Applier> getSurviveAppliers(String clusterId) {
        return currentMeta.getSurviveAppliers(clusterId);
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
    public void addResource(String clusterId, Releasable releasable) {
        currentMeta.addResource(clusterId, releasable);
    }

    @Override
    public void setSurviveReplicators(String registryKey, List<Replicator> surviveReplicators, Replicator activeReplicator) {
        currentMeta.setSurviveReplicators(registryKey, surviveReplicators, activeReplicator);
        notifyReplicatorActiveElected(registryKey, activeReplicator);
    }

    @Override
    public void setSurviveAppliers(String registryKey, List<Applier> surviveAppliers, Applier activeApplier) {
        currentMeta.setSurviveAppliers(registryKey, surviveAppliers, activeApplier);
        notifyApplierActiveElected(registryKey, activeApplier);
    }

    @Override
    public boolean watchReplicatorIfNotWatched(String registryKey) {
        return currentMeta.watchReplicatorIfNotWatched(registryKey);
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
            DbCluster clusterMeta = dcCache.getCluster(clusterId);
            List<Pair<String, String>> upstreamDcClusterIdList = getUpstreamDcClusterIdList(clusterMeta);
            for(Pair<String, String> upstreamDcClusterId : upstreamDcClusterIdList) {
                if(randomRoute(clusterId, upstreamDcClusterId.getKey()) != null) {
                    refreshApplierMaster(clusterMeta, upstreamDcClusterId.getValue());
                }
            }
        }
    }

    @VisibleForTesting
    protected List<Pair<String, String>> getUpstreamDcClusterIdList(DbCluster clusterMeta) {
        List<Pair<String, String>> upstreamDcClusterIdList = Lists.newArrayList();
        List<Applier> applierList = clusterMeta.getAppliers();

        for (Applier applier : applierList) {
            String targetMhaName = applier.getTargetMhaName();
            String targetName = applier.getTargetName();
            String targetIdc = applier.getTargetIdc();
            if (StringUtils.isNotBlank(targetMhaName) && StringUtils.isNotBlank(targetIdc)) {
                upstreamDcClusterIdList.add(new Pair<>(targetIdc, RegistryKey.from(targetName, targetMhaName)));
            }
        }

        return upstreamDcClusterIdList;
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

    public void setDcCache(DcCache dcCache) {
        this.dcCache = dcCache;
    }

    @VisibleForTesting
    protected void addStateChangeHandler(StateChangeHandler handler) {
        if(stateHandlers == null) {
            stateHandlers = Lists.newArrayList();
        }
        stateHandlers.add(handler);
    }
}
