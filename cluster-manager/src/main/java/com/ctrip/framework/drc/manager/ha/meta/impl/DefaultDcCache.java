package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparator;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.config.SourceProvider;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.framework.drc.manager.ha.meta.DcManager;
import com.ctrip.framework.drc.manager.ha.meta.comparator.*;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
@Component
public class DefaultDcCache extends AbstractLifecycleObservable implements DcCache, Runnable, TopElement {

    public static final String META_MODIFY_JUST_NOW_TEMPLATE = "current dc meta modifyTime {} larger than meta loadTime {}";

    public static String MEMORY_META_SERVER_DAO_KEY = "memory_meta_server_dao_file";

    public static int META_MODIFY_PROTECT_COUNT = 20;

    public static double META_REMOVE_THRESHOLD = 0.5;

    public static final String META_CHANGE_TYPE = "MetaChange";

    @Autowired(required = false)
    private SourceProvider sourceProvider;

    @Autowired
    private ClusterManagerConfig config;

    @Autowired
    private DataCenterService dataCenterService;

    private String currentDc;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Meta-Refresher"));

    private ScheduledFuture<?> future;

    private AtomicReference<DcManager> dcMetaManager = new AtomicReference<DcManager>(null);

    private AtomicLong metaModifyTime = new AtomicLong(System.currentTimeMillis());

    public DefaultDcCache() {
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        currentDc = dataCenterService.getDc();
        logger.info("[doInitialize][dc]{}", currentDc);
        this.dcMetaManager.set(loadMetaManager());
    }

    protected DcManager loadMetaManager() {

        DcManager dcMetaManager = null;
        if (sourceProvider != null) {
            try {
                logger.info("[loadMetaManager][load from console]");
                Dc dcMeta = sourceProvider.getDc(currentDc);
                dcMetaManager = DefaultDcManager.buildFromDcMeta(dcMeta);
            } catch (ResourceAccessException e) {
                logger.error("[loadMetaManager][consoleService]" + e.getMessage());
            } catch (Exception e) {
                logger.error("[loadMetaManager][consoleService]", e);
            }
        }

        if (dcMetaManager == null) {
            String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
            logger.info("[loadMetaManager][load from file]{}", fileName);
            dcMetaManager = DefaultDcManager.buildFromFile(currentDc, fileName);
        }

        logger.info("[loadMetaManager]{}", dcMetaManager);

        if (dcMetaManager == null) {
            throw new IllegalArgumentException("[loadMetaManager][fail]");
        }
        return dcMetaManager;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        future = scheduled.scheduleAtFixedRate(this, 0, config.getClusterRefreshMilli(), TimeUnit.MILLISECONDS);

    }

    @Override
    protected void doStop() throws Exception {

        future.cancel(true);
        super.doStop();
    }

    @Override
    public void run() {

        try {
            if (sourceProvider != null) {
                long metaLoadTime = System.currentTimeMillis();
                Dc future = sourceProvider.getDc(currentDc);
                Dc current = dcMetaManager.get().getDc();

                changeDcMeta(current, future, metaLoadTime);
                checkRouteChange(current, future);
            }
        } catch (Throwable th) {
            logger.error("[run]" + th.getMessage());
        }
    }

    @VisibleForTesting
    protected void changeDcMeta(Dc current, Dc future, final long metaLoadTime) {

        if (metaLoadTime <= metaModifyTime.get()) {
            logger.warn("[run][skip change dc meta]" + META_MODIFY_JUST_NOW_TEMPLATE, metaModifyTime.get(), metaLoadTime);
            return;
        }

        if(null == future) {
            logger.warn("[run][null new config]");
            return;
        }

        DcComparator dcMetaComparator = new DcComparator(current, future);
        dcMetaComparator.compare();

        logger.info("changed here: {}", dcMetaComparator);

        if(dcMetaComparator.getRemoved().size()/(current.getDbClusters().size()*1.0) > META_REMOVE_THRESHOLD) {
            logger.warn("[run][remove too many dbclusters]{}, {}, {}", META_REMOVE_THRESHOLD, dcMetaComparator.getRemoved().size(), dcMetaComparator);
            EventMonitor.DEFAULT.logAlertEvent("remove too many:" + dcMetaComparator.getRemoved().size());
            return;
        }

        DcManager newDcMetaManager = DefaultDcManager.buildFromDcMeta(future);
        boolean dcMetaUpdated = false;
        synchronized (this) {
            if (metaLoadTime > metaModifyTime.get()) {
                dcMetaManager.set(newDcMetaManager);
                dcMetaUpdated = true;
            }
        }

        if (!dcMetaUpdated) {
            logger.info("[run][skip change dc meta]" + META_MODIFY_JUST_NOW_TEMPLATE, metaModifyTime, metaLoadTime);
            return;
        }

        logger.info("[run][change dc meta]");
        if (dcMetaComparator.totalChangedCount() > 0) {
            logger.info("[run][change]{}", dcMetaComparator);
            EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("[add:%s, del:%s, mod:%s]",
                    StringUtil.join(",", (clusterMeta) -> clusterMeta.getId(), dcMetaComparator.getAdded()),
                    StringUtil.join(",", (clusterMeta) -> clusterMeta.getId(), dcMetaComparator.getRemoved()),
                    StringUtil.join(",", (comparator) -> comparator.idDesc(), dcMetaComparator.getMofified()))
            );
            notifyObservers(dcMetaComparator);
        }
    }

    private int drClusterNums(DcComparator comparator) {
        int result = 0;
        for(MetaComparator metaComparator : comparator.getMofified()) {
            ClusterComparator clusterMetaComparator = (ClusterComparator) metaComparator;
            ReplicatorComparator replicatorComparator = clusterMetaComparator.getReplicatorComparator();
            int replicatorChangeCount = replicatorComparator.totalChangedCount();
            result += replicatorChangeCount;
            logger.info("[ReplicatorComparator] change count {}", replicatorChangeCount);

            ApplierComparator applierComparator = clusterMetaComparator.getApplierComparator();
            int applierChangeCount = applierComparator.totalChangedCount();
            result += applierChangeCount;
            logger.info("[ApplierComparator] change count {}", applierChangeCount);
        }
        logger.info("[DR Switched][cluster num] {}", result);
        return result;
    }

    @VisibleForTesting
    protected void checkRouteChange(Dc current, Dc future) {
        DcRouteComparator comparator = new DcRouteComparator(current, future, IRoute.TAG_META);
        comparator.compare();

        if(!comparator.getRemoved().isEmpty()
                || !comparator.getMofified().isEmpty()) {
            notifyObservers(comparator);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public DcManager getDcMeta() {
        return this.dcMetaManager.get();
    }

    @Override
    public Set<String> getClusters() {
        return dcMetaManager.get().getClusters();
    }

    @Override
    public DbCluster getCluster(String clusterId) {
        return dcMetaManager.get().getCluster(clusterId);
    }

    @Override
    public Route randomRoute(String clusterId, String dstDc) {
        return dcMetaManager.get().randomRoute(clusterId, dstDc);
    }

    @Override
    public void clusterAdded(DbCluster clusterMeta) {

        EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("add:%s", clusterMeta.getId()));

        clusterModified(clusterMeta);
    }

    @Override
    public void clusterModified(DbCluster dbCluster) {

        EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("mod:%s", dbCluster.getId()));

        DbCluster current = dcMetaManager.get().getCluster(dbCluster.getId());
        dcMetaManager.get().update(dbCluster);

        logger.info("[clusterModified]{}, {}", current, dbCluster);
        DcComparator dcMetaComparator = DcComparator.buildClusterChanged(current, dbCluster);
        notifyObservers(dcMetaComparator);
    }

    @Override
    public void clusterDeleted(String registryKey) {

        EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("del:%s", registryKey));

        DbCluster clusterMeta = dcMetaManager.get().removeCluster(registryKey);
        logger.info("[clusterDeleted]{}", clusterMeta);
        DcComparator dcMetaComparator = DcComparator.buildClusterRemoved(clusterMeta);
        notifyObservers(dcMetaComparator);
    }

    @Override
    public Map<String, String> getBackupDcs(String clusterId) {
        Map<String, String> res = Maps.newHashMap();
        DbCluster dbCluster = getCluster(clusterId);
        List<Applier> applierList = dbCluster.getAppliers();
        for (Applier applier : applierList) {
            logger.info("[Applier] is {}", applier);
            String targetMhaName = applier.getTargetMhaName();
            String targetName = applier.getTargetName();
            if (StringUtils.isNotBlank(targetMhaName)) {
                res.put(applier.getTargetIdc(), RegistryKey.from(targetName, targetMhaName));
            }
        }

        return res;
    }
}
