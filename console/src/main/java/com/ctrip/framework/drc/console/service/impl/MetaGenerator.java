package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Service
public class MetaGenerator {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    @Autowired
    private DataCenterService dataCenterService;
    
    private DalUtils dalUtils = DalUtils.getInstance();

    protected String localDcName;
    private List<BuTbl> buTbls;
    private List<RouteTbl> routeTbls;
    private List<ProxyTbl> proxyTbls;
    private List<ClusterMhaMapTbl> clusterMhaMapTbls;
    private List<GroupMappingTbl> groupMappingTbls;
    private List<DcTbl> dcTbls;
    private List<ClusterTbl> clusterTbls;
    private List<MhaGroupTbl> mhaGroupTbls;
    private List<MhaTbl> mhaTbls;
    private List<ResourceTbl> resourceTbls;
    private List<MachineTbl> machineTbls;
    private List<ReplicatorGroupTbl> replicatorGroupTbls;
    private List<ApplierGroupTbl> applierGroupTbls;
    private List<ClusterManagerTbl> clusterManagerTbls;
    private List<ZookeeperTbl> zookeeperTbls;
    private List<ReplicatorTbl> replicatorTbls;
    private List<ApplierTbl> applierTbls;

    public Drc getDrc() throws Exception {
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if(publicCloudDc.contains(dataCenterService.getDc())){
            return null;
        }
        List<DcTbl> dcTbls = dalUtils.getDcTblDao().queryAll();
        Drc drc = new Drc();
        for(DcTbl dcTbl : dcTbls) {
            generateDc(drc, dcTbl.getId(), dcTbl.getDcName());
        }
        logger.debug("current DRC: {}", drc);
        return drc;
    }

    private Dc generateDcFrame(Drc drc, String dcName) {
        logger.debug("generate dc: {}", dcName);
        Dc dc = new Dc(dcName);
        drc.addDc(dc);
        return dc;
    }

    private void generateClusterManager(Dc dc, Long dcId) {
        List<ResourceTbl> cmResources = resourceTbls.stream()
                .filter(resourceTbl -> resourceTbl.getType().equals(ModuleEnum.CLUSTER_MANAGER.getCode()) && resourceTbl.getDcId().equals(dcId))
                .collect(Collectors.toList());
        for(ResourceTbl cmResource : cmResources) {
            Long resourceId = cmResource.getId();
            String resourceIp = cmResource.getIp();
            ClusterManagerTbl clusterManagerTbl = clusterManagerTbls.stream().filter(cmTbl -> cmTbl.getResourceId().equals(resourceId)).findFirst().get();
            logger.debug("generate cm: {}", resourceIp);
            ClusterManager clusterManager = new ClusterManager();
            clusterManager.setIp(resourceIp)
                    .setPort(clusterManagerTbl.getPort())
                    .setMaster(clusterManagerTbl.getMaster().equals(BooleanEnum.TRUE.getCode()));
            dc.addClusterManager(clusterManager);
        }
    }

    private void generateRoute(Dc dc, Long dcId) {
        List<RouteTbl> localRouteTbls = routeTbls.stream()
                .filter(routeTbl -> routeTbl.getSrcDcId().equals(dcId))
                .collect(Collectors.toList());
        for(RouteTbl routeTbl : localRouteTbls) {
            logger.info("generate route id: {}", routeTbl.getId());
            String srcDc = dcTbls.stream().filter(p -> p.getId().equals(routeTbl.getSrcDcId())).findFirst().get().getDcName();
            String dstDc = dcTbls.stream().filter(p -> p.getId().equals(routeTbl.getDstDcId())).findFirst().get().getDcName();
            logger.info("generate route: {}-{},{}->{}", routeTbl.getRouteOrgId(), routeTbl.getTag(), srcDc, dstDc);
            Route route = new Route();
            route.setId(routeTbl.getId().intValue());
            route.setOrgId(routeTbl.getRouteOrgId().intValue());
            route.setSrcDc(srcDc);
            route.setDstDc(dstDc);
            route.setRouteInfo(generateRouteInfo(routeTbl.getSrcProxyIds(), routeTbl.getOptionalProxyIds(), routeTbl.getDstProxyIds()));
            route.setTag(routeTbl.getTag());
            dc.addRoute(route);
        }
    }

    private String generateRouteInfo(String srcProxyIds, String relayProxyIds, String dstProxyIds) {
        List<String> srcProxyUris = getProxyUris(srcProxyIds);
        List<String> relayProxyUris = getProxyUris(relayProxyIds);
        List<String> dstProxyUris = getProxyUris(dstProxyIds);

        List<String> route = Lists.newArrayList();
        for (List<String> proxyUris : Arrays.asList(srcProxyUris, relayProxyUris, dstProxyUris)) {
            if (proxyUris.size() != 0) {
                route.add(StringUtils.join(proxyUris, ","));
            }
        }
        return StringUtils.join(route, " ");
    }

    private List<String> getProxyUris(String proxyIds) {
        List<String> proxyUris = Lists.newArrayList();
        if(StringUtils.isNotBlank(proxyIds)) {
            String[] proxyIdArr = proxyIds.split(",");
            for(String idStr : proxyIdArr) {
                Long proxyId = Long.parseLong(idStr);
                proxyTbls.stream().filter(p -> p.getId().equals(proxyId)).findFirst().ifPresent(proxyTbl -> proxyUris.add(proxyTbl.getUri()));
            }
        }
        return proxyUris;
    }

    private void generateZk(Dc dc, Long dcId) {
        List<ResourceTbl> zkResources = resourceTbls.stream()
                .filter(resourceTbl -> resourceTbl.getType().equals(ModuleEnum.ZOOKEEPER.getCode()) && resourceTbl.getDcId().equals(dcId))
                .collect(Collectors.toList());
        StringBuilder zkAddrBuilder = new StringBuilder();
        Iterator<ResourceTbl> zkIterator = zkResources.iterator();
        while(zkIterator.hasNext()) {
            ResourceTbl zkResource = zkIterator.next();
            Long resourceId = zkResource.getId();
            String resourceIp = zkResource.getIp();
            ZookeeperTbl zookeeperTbl = zookeeperTbls.stream().filter(zkTbl -> zkTbl.getResourceId().equals(resourceId)).findFirst().get();
            zkAddrBuilder.append(resourceIp+':'+zookeeperTbl.getPort());
            if(zkIterator.hasNext()) {
                zkAddrBuilder.append(',');
            }
        }
        String zkAddr = zkAddrBuilder.toString();
        logger.debug("generate zk: {}", zkAddr);
        ZkServer zkServer = new ZkServer();
        zkServer.setAddress(zkAddr);
        dc.setZkServer(zkServer);
    }

    private DbCluster generateDbCluster(Dc dc, MhaTbl mhaTbl) {
        // header tag
        String mhaName = mhaTbl.getMhaName();
        ClusterMhaMapTbl clusterMhaMapTbl = clusterMhaMapTbls.stream().filter(cMMapTbl -> cMMapTbl.getMhaId().equals(mhaTbl.getId())).findFirst().get();
        Long clusterId = clusterMhaMapTbl.getClusterId();
        ClusterTbl clusterTbl = clusterTbls.stream().filter(cTbl -> cTbl.getId().equals(clusterId)).findFirst().get();
        String clusterName = clusterTbl.getClusterName();
        BuTbl buTbl = buTbls.stream().filter(predicate -> predicate.getId().equals(clusterTbl.getBuId())).findFirst().get();
        logger.debug("generate dbCluster for mha: {}", mhaName);
        DbCluster dbCluster = new DbCluster();
        dbCluster.setId(clusterName+'.'+mhaName)
                .setName(clusterName)
                .setMhaName(mhaName)
                .setBuName(buTbl.getBuName())
                .setAppId(clusterTbl.getClusterAppId())
                .setOrgId(clusterTbl.getBuId().intValue());
        dc.addDbCluster(dbCluster);
        return dbCluster;
    }

    private Dbs generateDbs(DbCluster dbCluster, MhaGroupTbl mhaGroupTbl, MhaTbl mhaTbl) {
        logger.debug("generate dbs for mha: {}", mhaTbl.getMhaName());
        Dbs dbs = new Dbs();
        dbs.setReadUser(mhaGroupTbl.getReadUser())
                .setReadPassword(mhaGroupTbl.getReadPassword())
                .setWriteUser(mhaGroupTbl.getWriteUser())
                .setWritePassword(mhaGroupTbl.getWritePassword())
                .setMonitorUser(mhaGroupTbl.getMonitorUser())
                .setMonitorPassword(mhaGroupTbl.getMonitorPassword());
        dbCluster.setDbs(dbs);
        return dbs;
    }

    private void generateDb(Dbs dbs, MhaTbl mhaTbl) {
        List<MachineTbl> curMhaMachineTbls = machineTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        for(MachineTbl machineTbl : curMhaMachineTbls) {
            logger.debug("generate machine: {} for mha: {}", machineTbl.getIp(), mhaTbl.getMhaName());
            Db db = new Db();
            db.setIp(machineTbl.getIp())
                    .setPort(machineTbl.getPort())
                    .setMaster(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                    .setUuid(machineTbl.getUuid());
            dbs.addDb(db);
        }
    }

    private void generateReplicators(DbCluster dbCluster, MhaTbl mhaTbl) {
        //  replicators
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaTbl.getId())).findFirst().orElse(null);
        if(null != replicatorGroupTbl) {
            List<ReplicatorTbl> curMhaReplicators = replicatorTbls.stream().filter(predicate -> predicate.getRelicatorGroupId().equals(replicatorGroupTbl.getId())).collect(Collectors.toList());
            for(ReplicatorTbl replicatorTbl : curMhaReplicators) {
                ResourceTbl resourceTbl = resourceTbls.stream().filter(predicate -> predicate.getId().equals(replicatorTbl.getResourceId())).findFirst().get();
                logger.debug("generate replicator: {}:{} for mha: {}", resourceTbl.getIp(), replicatorTbl.getApplierPort(), mhaTbl.getMhaName());
                Replicator replicator = new Replicator();
                replicator.setIp(resourceTbl.getIp())
                        .setPort(replicatorTbl.getPort())
                        .setApplierPort(replicatorTbl.getApplierPort())
                        .setExcludedTables(replicatorGroupTbl.getExcludedTables())
                        .setGtidSkip(replicatorTbl.getGtidInit());
                dbCluster.addReplicator(replicator);
            }
        }
    }

    private void generateAppliers(DbCluster dbCluster, MhaTbl mhaTbl) {
        List<ApplierGroupTbl> applierGroupTblList = applierGroupTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        for (ApplierGroupTbl applierGroupTbl : applierGroupTblList) {
            generateApplierInstances(dbCluster, mhaTbl, applierGroupTbl);
        }
    }

    private void generateApplierInstances(DbCluster dbCluster, MhaTbl mhaTbl, ApplierGroupTbl applierGroupTbl) {
        // simple implementation, duo repl only
        ReplicatorGroupTbl targetReplicatorGroupTbl = replicatorGroupTbls.stream().filter(predicate -> predicate.getId().equals(applierGroupTbl.getReplicatorGroupId())).findFirst().get();
        MhaTbl targetMhaTbl = mhaTbls.stream().filter(predicate -> predicate.getId().equals(targetReplicatorGroupTbl.getMhaId())).findFirst().get();
        DcTbl targetDcTbl = dcTbls.stream().filter(predicte -> predicte.getId().equals(targetMhaTbl.getDcId())).findFirst().get();
        List<ApplierTbl> curMhaAppliers = applierTbls.stream().filter(predicate -> predicate.getApplierGroupId().equals(applierGroupTbl.getId())).collect(Collectors.toList());
        for(ApplierTbl applierTbl : curMhaAppliers) {
            ResourceTbl resourceTbl = resourceTbls.stream().filter(predicate -> predicate.getId().equals(applierTbl.getResourceId())).findFirst().get();
            logger.debug("generate applier: {} for mha: {}", resourceTbl.getIp(), mhaTbl.getMhaName());
            Applier applier = new Applier();
            applier.setIp(resourceTbl.getIp())
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(targetDcTbl.getDcName())
                    .setTargetMhaName(targetMhaTbl.getMhaName())
                    .setGtidExecuted(applierTbl.getGtidInit())
                    .setIncludedDbs(applierGroupTbl.getIncludedDbs())
                    .setNameFilter(applierGroupTbl.getNameFilter())
                    .setTargetName(applierGroupTbl.getTargetName())
                    .setApplyMode(applierGroupTbl.getApplyMode());
            dbCluster.addApplier(applier);
        }
    }

    private void generateDbClusters(Dc dc, Long dcId) {
        List<MhaTbl> localMhaTbls = mhaTbls.stream().filter(mhaTbl -> (mhaTbl.getDcId().equals(dcId))).collect(Collectors.toList());
        for(MhaTbl mhaTbl : localMhaTbls) {
            Set<Long> mhaGroupIds = groupMappingTbls.stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && p.getMhaId().equals(mhaTbl.getId())).map(GroupMappingTbl::getMhaGroupId).collect(Collectors.toSet());
            List<MhaGroupTbl> mhaGroupTblList = mhaGroupTbls.stream()
                    .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaGroupIds.contains(p.getId()) && p.getDrcEstablishStatus().equals(EstablishStatusEnum.ESTABLISHED.getCode()))
                    .collect(Collectors.toList());

            if(mhaGroupTblList.size() == 0) {
                logger.debug("skip, mha group for {} is not established yet", mhaTbl.getMhaName());
                continue;
            }
            DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
            Dbs dbs = generateDbs(dbCluster, mhaGroupTblList.iterator().next(), mhaTbl);
            generateDb(dbs, mhaTbl);
            generateReplicators(dbCluster, mhaTbl);
            generateAppliers(dbCluster, mhaTbl);
        }
    }

    private void generateDc(Drc drc, Long dcId, String dcName) throws Exception {
        DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.meta", dcName, new Task() {
            @Override
            public void go() throws Exception {
                refreshMetaData();
                Dc dc = generateDcFrame(drc, dcName);
                generateRoute(dc, dcId);
                generateClusterManager(dc, dcId);
                generateZk(dc, dcId);
                generateDbClusters(dc, dcId);
            }
        });
    }

    private void refreshMetaData() throws SQLException {
        buTbls = dalUtils.getBuTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        routeTbls = dalUtils.getRouteTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        proxyTbls = dalUtils.getProxyTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        clusterMhaMapTbls = dalUtils.getClusterMhaMapTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        groupMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        dcTbls = dalUtils.getDcTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        clusterTbls = dalUtils.getClusterTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        mhaGroupTbls = dalUtils.getMhaGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        mhaTbls = dalUtils.getMhaTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        resourceTbls = dalUtils.getResourceTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        machineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        replicatorGroupTbls = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        applierGroupTbls = dalUtils.getApplierGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        clusterManagerTbls = dalUtils.getClusterManagerTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        zookeeperTbls = dalUtils.getZookeeperTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        applierTbls = dalUtils.getApplierTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
    }

    public List<BuTbl> getBuTbls() {
        return buTbls;
    }

    public List<ClusterMhaMapTbl> getClusterMhaMapTbls() {
        return clusterMhaMapTbls;
    }

    public List<GroupMappingTbl> getGroupMappingTbls() { return groupMappingTbls; }

    public List<DcTbl> getDcTbls() {
        return dcTbls;
    }

    public List<ClusterTbl> getClusterTbls() {
        return clusterTbls;
    }

    public List<MhaGroupTbl> getMhaGroupTbls() {
        return mhaGroupTbls;
    }

    public List<MhaTbl> getMhaTbls() {
        return mhaTbls;
    }

    public List<ResourceTbl> getResourceTbls() {
        return resourceTbls;
    }

    public List<MachineTbl> getMachineTbls() {
        return machineTbls;
    }

    public List<ReplicatorGroupTbl> getReplicatorGroupTbls() {
        return replicatorGroupTbls;
    }

    public List<ApplierGroupTbl> getApplierGroupTbls() {
        return applierGroupTbls;
    }

    public List<ClusterManagerTbl> getClusterManagerTbls() {
        return clusterManagerTbls;
    }

    public List<ZookeeperTbl> getZookeeperTbls() {
        return zookeeperTbls;
    }

    public List<ReplicatorTbl> getReplicatorTbls() {
        return replicatorTbls;
    }

    public List<ApplierTbl> getApplierTbls() {
        return applierTbls;
    }

    public List<DdlHistoryTbl> getDdlHistoryTbls() throws SQLException {
        return dalUtils.getDdlHistoryTblDao().queryAll();
    }

    public List<DataConsistencyMonitorTbl> getDataConsistencyMonitorTbls() throws SQLException {
        return dalUtils.getDataConsistencyMonitorTblDao().queryAll();
    }
}
