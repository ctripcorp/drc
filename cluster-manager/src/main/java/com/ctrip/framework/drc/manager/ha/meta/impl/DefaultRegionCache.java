package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.manager.config.DataCenterService;
import com.ctrip.framework.drc.manager.config.SourceProvider;
import com.ctrip.framework.drc.manager.ha.config.ClusterManagerConfig;
import com.ctrip.framework.drc.manager.ha.meta.DcCache;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author limingdong
 * @create 2022/6/22
 */
@Component
public class DefaultRegionCache extends AbstractLifecycleObservable implements DcCache, TopElement {

    @Autowired(required = false)
    private SourceProvider sourceProvider;

    @Autowired
    private ClusterManagerConfig config;

    @Autowired
    private DataCenterService dataCenterService;

    private String currentRegion;

    private List<DefaultDcCache> dcCaches = Lists.newArrayList();

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        currentRegion = dataCenterService.getRegion();
        logger.info("[doInitialize][region]{}", currentRegion);
        Set<String> idcLists = dataCenterService.getRegionIdcMapping().get(currentRegion);
        for (String idc : idcLists) {
            DefaultDcCache dcCache = new DefaultDcCache(config, sourceProvider, idc);
            dcCache.initialize();
            dcCaches.add(dcCache);
        }
    }

    @Override
    protected void doStart() throws Exception {
        for (DefaultDcCache dcCache : dcCaches) {
            dcCache.start();
        }
    }

    @Override
    protected void doStop() throws Exception {
        for (DefaultDcCache dcCache : dcCaches) {
            dcCache.stop();
        }
    }

    @Override
    public Set<String> getClusters() {
        return dcCaches.stream().flatMap(dcCache -> dcCache.getClusters().stream()).collect(Collectors.toSet());
    }

    @Override
    public Map<String, String> getBackupDcs(String clusterId) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> !dcCache.getBackupDcs(clusterId).isEmpty()).findFirst();
        return defaultDcCache.isPresent() ? defaultDcCache.get().getBackupDcs(clusterId) : Maps.newHashMap();
    }

    @Override
    public DbCluster getCluster(String registryKey) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> dcCache.getCluster(registryKey) != null).findFirst();
        return defaultDcCache.isPresent() ? defaultDcCache.get().getCluster(registryKey) : null;
    }

    @Override
    public Route randomRoute(String clusterId, String dstDc) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> dcCache.randomRoute(clusterId, dstDc) != null).findFirst();
        return defaultDcCache.isPresent() ? defaultDcCache.get().randomRoute(clusterId, dstDc) : null;
    }

    @Override
    public void clusterAdded(DbCluster dbCluster) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> dcCache.getCluster(dbCluster.getId()) != null).findFirst();
        if (defaultDcCache.isPresent()) {
            defaultDcCache.get().clusterAdded(dbCluster);
        } else {
            int random = new Random().nextInt(dcCaches.size());
            dcCaches.get(random).clusterAdded(dbCluster);
        }
    }

    @Override
    public void clusterModified(DbCluster dbCluster) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> dcCache.getCluster(dbCluster.getId()) != null).findFirst();
        defaultDcCache.ifPresent(dcCache -> dcCache.clusterModified(dbCluster));
    }

    @Override
    public void clusterDeleted(String registryKey) {
        Optional<DefaultDcCache> defaultDcCache = dcCaches.stream().filter(dcCache -> dcCache.getCluster(registryKey) != null).findFirst();
        defaultDcCache.ifPresent(dcCache -> dcCache.clusterDeleted(registryKey));
    }
}
