package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.framework.drc.manager.ha.meta.DrcManager;
import com.ctrip.xpipe.utils.FileUtils;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public class DefaultDrcManager extends AbstractMetaManager implements DrcManager {

    private String fileName = null;

    protected final Drc drc;

    public DefaultDrcManager(Drc drc){
        this.drc = drc;
    }

    private DefaultDrcManager(String fileName) {
        this.fileName = fileName;
        drc = load(fileName);
    }

    public static DrcManager buildFromFile(String fileName){
        return new DefaultDrcManager(fileName);
    }

    public static DrcManager buildFromMeta(Drc drc){
        return new DefaultDrcManager(drc);
    }

    public Drc load(String fileName) {

        try {
            InputStream ins = FileUtils.getFileInputStream(fileName);
            return DefaultSaxParser.parse(ins);
        } catch (SAXException | IOException e) {
            logger.error("[load]" + fileName, e);
            throw new IllegalStateException("load " + fileName + " failed!", e);
        }
    }

    @Override
    public Set<String> getDcs() {
        return drc.getDcs().keySet();
    }

    @Override
    public Set<String> getDcClusters(String dc) {
        return getDirectDcMeta(dc).getDbClusters().keySet();
    }

    @Override
    public DbCluster getCluster(String dc, String registryKey) {
        return clone(getDirectDbCluster(dc, registryKey));
    }

    @Override
    public Route randomRoute(String currentDc, String tag, Integer orgId, String dstDc) {
        Dc dc = getDirectDcMeta(currentDc);
        return RouteUtils.randomRoute(currentDc, tag, orgId, dstDc, dc);
    }

    @Override
    public List<ClusterManager> getClusterManagerServers(String dc) {
        Dc dcMeta = getDirectDcMeta(dc);
        if( dcMeta == null ){
            return null;
        }
        return clone(new LinkedList<>(dcMeta.getClusterManagers()));
    }

    @Override
    public ZkServer getZkServer(String dc) {
        Dc dcMeta = getDirectDcMeta(dc);
        if( dcMeta == null ){
            return null;
        }
        return clone(dcMeta.getZkServer());
    }

    @Override
    public Dc getDc(String dc) {
        return clone(getDirectDcMeta(dc));
    }

    @Override
    public boolean hasCluster(String currentDc, String registryKey) {
        return getDirectDbCluster(currentDc, registryKey) != null;
    }

    protected Dc getDirectDcMeta(String dcName) {

        return drc.getDcs().get(dcName);
    }

    public DbCluster getDirectDbCluster(String dc, String registryKey) {

        Dc dcMeta = getDirectDcMeta(dc);
        if(dcMeta == null){
            return null;
        }
        return dcMeta.getDbClusters().get(registryKey);
    }

    @Override
    public DbCluster removeCluster(String currentDc, String registryKey) {

        DbCluster dbCluster = getDirectDbCluster(currentDc, registryKey);
        if (dbCluster != null) {
            Dc dcMeta = getDirectDcMeta(currentDc);
            dcMeta.getDbClusters().remove(dbCluster);
        }
        return dbCluster;
    }

    @Override
    public void update(String dcId, DbCluster clusterMeta) {
        Dc dcMeta = drc.getDcs().get(dcId);
        dcMeta.addDbCluster(clone(clusterMeta));
    }
}
