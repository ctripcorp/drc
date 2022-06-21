package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.meta.DcManager;
import com.ctrip.framework.drc.manager.ha.meta.DrcManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class DefaultDcManager implements DcManager {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    private DrcManager drcManager;

    private String currentDc;

    private DefaultDcManager(String currentDc, DrcManager drcManager){
        this.drcManager = drcManager;
        this.currentDc = currentDc;
    }


    public static DcManager buildFromFile(String dcId, String fileName){

        return new DefaultDcManager(dcId, DefaultDrcManager.buildFromFile(fileName));
    }

    public static DcManager buildForDc(String dcId){

        Drc drc = new Drc();
        Dc dcMeta = new Dc();
        dcMeta.setId(dcId);
        drc.addDc(dcMeta);
        return new DefaultDcManager(dcId, DefaultDrcManager.buildFromMeta(drc));
    }

    public static DcManager buildFromDcMeta(Dc dcMeta){

        Drc drc = new Drc();
        drc.addDc(dcMeta);
        return new DefaultDcManager(dcMeta.getId(), DefaultDrcManager.buildFromMeta(drc));
    }

    @Override
    public Set<String> getClusters() {
        return drcManager.getDcClusters(currentDc);
    }

    @Override
    public Route randomRoute(String clusterId, String dstDc) {

        DbCluster clusterMeta = drcManager.getCluster(currentDc, clusterId);
        if(clusterMeta == null){
            throw new IllegalArgumentException("clusterId not exist:" + clusterId);
        }

        return drcManager.metaRandomRoutes(currentDc, clusterMeta.getOrgId(), dstDc);
    }

    @Override
    public boolean hasCluster(String registryKey) {
        return drcManager.hasCluster(currentDc, registryKey);
    }

    @Override
    public DbCluster getCluster(String registryKey) {
        return drcManager.getCluster(currentDc, registryKey);
    }

    @Override
    public Dc getDc() {
        return MetaClone.clone(drcManager.getDc(currentDc));
    }

    @Override
    public void update(DbCluster dbCluster) {
        drcManager.update(currentDc, dbCluster);
    }

    @Override
    public DbCluster removeCluster(String registryKey) {
        return drcManager.removeCluster(currentDc, registryKey);
    }
}
