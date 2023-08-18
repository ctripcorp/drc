package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.utils.convert.TableNameBuilder;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/25 14:09
 */
@Service
public class MetaGeneratorV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MessengerServiceV2 messengerService;
    @Autowired
    private DataMediaServiceV2 dataMediaService;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ClusterManagerTblDao clusterManagerTblDao;
    @Autowired
    private ZookeeperTblDao zookeeperTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;

    private List<BuTbl> buTbls;
    private List<RouteTbl> routeTbls;
    private List<ProxyTbl> proxyTbls;
    private List<DcTbl> dcTbls;
    private List<MhaTblV2> mhaTbls;
    private List<ResourceTbl> resourceTbls;
    private List<MachineTbl> machineTbls;
    private List<ReplicatorGroupTbl> replicatorGroupTbls;
    private List<ApplierGroupTblV2> applierGroupTbls;
    private List<ClusterManagerTbl> clusterManagerTbls;
    private List<ZookeeperTbl> zookeeperTbls;
    private List<ReplicatorTbl> replicatorTbls;
    private List<ApplierTblV2> applierTbls;
    private List<MhaReplicationTbl> mhaReplicationTbls;
    private List<MhaDbMappingTbl> mhaDbMappingTbls;
    private List<DbReplicationTbl> dbReplicationTbls;
    private List<DbTbl> dbTbls;

    public Drc getDrc() throws Exception {
        Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
        if (publicCloudRegion.contains(consoleConfig.getRegion())) {
            return null;
        }
        List<DcTbl> dcTbls = dcTblDao.queryAll();
        Drc drc = new Drc();
        for (DcTbl dcTbl : dcTbls) {
            generateDc(drc, dcTbl);
        }
        logger.debug("current DRC: {}", drc);
        return drc;
    }

    private void generateDc(Drc drc, DcTbl dcTbl) throws Exception {
        DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.meta", dcTbl.getDcName(), new Task() {
            @Override
            public void go() throws Exception {
                refreshMetaData();
                Dc dc = generateDcFrame(drc, dcTbl);
                generateRoute(dc, dcTbl.getId());
                generateClusterManager(dc, dcTbl.getId());
                generateZk(dc, dcTbl.getId());
                generateDbClusters(dc, dcTbl.getId());
            }
        });
    }

    private void refreshMetaData() throws SQLException {
        buTbls = buTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        routeTbls = routeTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        proxyTbls = proxyTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        dcTbls = dcTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        resourceTbls = resourceTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        machineTbls = machineTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        clusterManagerTbls = clusterManagerTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        zookeeperTbls = zookeeperTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        replicatorTbls = replicatorTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        applierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        dbReplicationTbls = dbReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
    }

    private Dc generateDcFrame(Drc drc, DcTbl dcTbl) {
        logger.debug("generate dc: {}", dcTbl.getDcName());
        Dc dc = new Dc(dcTbl.getDcName());
        dc.setRegion(dcTbl.getRegionName());
        drc.addDc(dc);
        return dc;
    }

    private void generateRoute(Dc dc, Long dcId) {
        List<RouteTbl> localRouteTbls = routeTbls.stream()
                .filter(routeTbl -> routeTbl.getSrcDcId().equals(dcId))
                .collect(Collectors.toList());

        for (RouteTbl routeTbl : localRouteTbls) {
            logger.info("generate route id: {}", routeTbl.getId());
            Route route = new Route();
            route.setId(routeTbl.getId().intValue());
            route.setOrgId(routeTbl.getRouteOrgId().intValue());

            route.setRouteInfo(generateRouteInfo(routeTbl.getSrcProxyIds(), routeTbl.getOptionalProxyIds(), routeTbl.getDstProxyIds()));
            route.setTag(routeTbl.getTag());
            dc.addRoute(route);

            Optional<DcTbl> srcDcTbl = dcTbls.stream().filter(p -> p.getId().equals(routeTbl.getSrcDcId())).findFirst();
            Optional<DcTbl> dstDcTbl = dcTbls.stream().filter(p -> p.getId().equals(routeTbl.getDstDcId())).findFirst();
            String srcDc = srcDcTbl.map(DcTbl::getDcName).orElse(StringUtils.EMPTY);
            String dstDc = dstDcTbl.map(DcTbl::getDcName).orElse(StringUtils.EMPTY);
            route.setSrcDc(srcDc);
            route.setDstDc(dstDc);

            route.setSrcRegion(srcDcTbl.map(DcTbl::getRegionName).orElse(StringUtils.EMPTY));
            route.setDstRegion(dstDcTbl.map(DcTbl::getRegionName).orElse(StringUtils.EMPTY));
            logger.info("generate route: {}-{},{}->{}", routeTbl.getRouteOrgId(), routeTbl.getTag(), srcDc, dstDc);
        }
    }

    private void generateClusterManager(Dc dc, Long dcId) {
        List<ResourceTbl> cmResources = resourceTbls.stream()
                .filter(resourceTbl -> resourceTbl.getType().equals(ModuleEnum.CLUSTER_MANAGER.getCode()) && resourceTbl.getDcId().equals(dcId))
                .collect(Collectors.toList());
        for (ResourceTbl cmResource : cmResources) {
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

    private void generateZk(Dc dc, Long dcId) {
        List<ResourceTbl> zkResources = resourceTbls.stream()
                .filter(resourceTbl -> resourceTbl.getType().equals(ModuleEnum.ZOOKEEPER.getCode()) && resourceTbl.getDcId().equals(dcId))
                .collect(Collectors.toList());
        StringBuilder zkAddressBuilder = new StringBuilder();
        Iterator<ResourceTbl> zkIterator = zkResources.iterator();
        while (zkIterator.hasNext()) {
            ResourceTbl zkResource = zkIterator.next();
            Long resourceId = zkResource.getId();
            String resourceIp = zkResource.getIp();
            ZookeeperTbl zookeeperTbl = zookeeperTbls.stream().filter(zkTbl -> zkTbl.getResourceId().equals(resourceId)).findFirst().get();
            zkAddressBuilder.append(resourceIp + ':' + zookeeperTbl.getPort());
            if (zkIterator.hasNext()) {
                zkAddressBuilder.append(',');
            }
        }
        String zkAddress = zkAddressBuilder.toString();
        logger.debug("generate zk: {}", zkAddress);
        ZkServer zkServer = new ZkServer();
        zkServer.setAddress(zkAddress);
        dc.setZkServer(zkServer);
    }

    private void generateDbClusters(Dc dc, Long dcId) throws SQLException {
        List<MhaTblV2> localMhaTbls = mhaTbls.stream().filter(mhaTbl -> (mhaTbl.getDcId().equals(dcId))).collect(Collectors.toList());
        for (MhaTblV2 mhaTbl : localMhaTbls) {
            DbCluster dbCluster = generateDbCluster(dc, mhaTbl);
            Dbs dbs = generateDbs(dbCluster, mhaTbl);
            generateDb(dbs, mhaTbl);
            generateReplicators(dbCluster, mhaTbl);
            generateMessengers(dbCluster, mhaTbl);
            generateAppliers(dbCluster, mhaTbl);
        }
    }

    private DbCluster generateDbCluster(Dc dc, MhaTblV2 mhaTbl) {
        String mhaName = mhaTbl.getMhaName();
        logger.debug("generate dbCluster for mha: {}", mhaName);

        DbCluster dbCluster = new DbCluster();
        dbCluster.setId(mhaTbl.getClusterName() + '.' + mhaName)
                .setName(mhaTbl.getClusterName())
                .setMhaName(mhaName)
                .setBuName(buTbls.stream().filter(e -> e.getId().equals(mhaTbl.getBuId())).findFirst().map(BuTbl::getBuName).orElse(StringUtils.EMPTY))
                .setAppId(mhaTbl.getAppId())
                .setOrgId(mhaTbl.getBuId().intValue())
                .setApplyMode(mhaTbl.getApplyMode());
        dc.addDbCluster(dbCluster);
        return dbCluster;
    }

    private Dbs generateDbs(DbCluster dbCluster, MhaTblV2 mhaTbl) {
        logger.debug("generate dbs for mha: {}", mhaTbl.getMhaName());
        Dbs dbs = new Dbs();
        dbs.setReadUser(mhaTbl.getReadUser())
                .setReadPassword(mhaTbl.getReadPassword())
                .setWriteUser(mhaTbl.getWriteUser())
                .setWritePassword(mhaTbl.getWritePassword())
                .setMonitorUser(mhaTbl.getMonitorUser())
                .setMonitorPassword(mhaTbl.getMonitorPassword());
        dbCluster.setDbs(dbs);
        return dbs;
    }

    private void generateDb(Dbs dbs, MhaTblV2 mhaTbl) {
        List<MachineTbl> mhaMachineTblList = machineTbls.stream().filter(e -> e.getMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        for (MachineTbl machineTbl : mhaMachineTblList) {
            logger.debug("generate machine: {} for mha: {}", machineTbl.getIp(), mhaTbl.getMhaName());
            Db db = new Db();
            db.setIp(machineTbl.getIp())
                    .setPort(machineTbl.getPort())
                    .setMaster(machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode()))
                    .setUuid(machineTbl.getUuid());
            dbs.addDb(db);
        }
    }

    private void generateReplicators(DbCluster dbCluster, MhaTblV2 mhaTbl) {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTbls.stream().filter(e -> e.getMhaId().equals(mhaTbl.getId())).findFirst().orElse(null);
        if (null != replicatorGroupTbl) {
            List<ReplicatorTbl> curMhaReplicators = replicatorTbls.stream().filter(e -> e.getRelicatorGroupId().equals(replicatorGroupTbl.getId())).collect(Collectors.toList());
            for (ReplicatorTbl replicatorTbl : curMhaReplicators) {
                ResourceTbl resourceTbl = resourceTbls.stream().filter(e -> e.getId().equals(replicatorTbl.getResourceId())).findFirst().get();
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

    private void generateMessengers(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
        List<Messenger> messengers = messengerService.generateMessengers(mhaTbl.getId());
        for (Messenger messenger : messengers) {
            dbCluster.addMessenger(messenger);
        }
    }

    private void generateAppliers(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
        List<MhaReplicationTbl> mhaReplicationTblList = mhaReplicationTbls.stream().filter(e -> e.getDstMhaId().equals(mhaTbl.getId())).collect(Collectors.toList());
        List<Long> mhaReplicationIds = mhaReplicationTblList.stream().map(MhaReplicationTbl::getId).collect(Collectors.toList());
        List<ApplierGroupTblV2> applierGroupTblList = applierGroupTbls.stream()
                .filter(e -> mhaReplicationIds.contains(e.getMhaReplicationId()))
                .collect(Collectors.toList());
        Map<Long, MhaReplicationTbl> mhaReplicationTblMap = mhaReplicationTblList.stream().collect(Collectors.toMap(MhaReplicationTbl::getId, Function.identity(), (k1, k2) -> k1));

        for (ApplierGroupTblV2 applierGroupTbl : applierGroupTblList) {
            MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblMap.get(applierGroupTbl.getMhaReplicationId());
            MhaTblV2 srcMhatbl = mhaTbls.stream().filter(e -> e.getId().equals(mhaReplicationTbl.getSrcMhaId())).findFirst().get();
            generateApplierInstances(dbCluster, srcMhatbl, mhaTbl, applierGroupTbl);
        }
    }

    private void generateApplierInstances(DbCluster dbCluster, MhaTblV2 srcMhatbl, MhaTblV2 dstMhaTbl, ApplierGroupTblV2 applierGroupTbl) throws SQLException {
        List<MhaDbMappingTbl> srcMhaDbMappingTbls = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(srcMhatbl.getId())).collect(Collectors.toList());
        List<MhaDbMappingTbl> dstMhaDbMappingTbls = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(dstMhaTbl.getId())).collect(Collectors.toList());
        List<Long> srcMhaDbMappingIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<Long> srcDbIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> dstDbIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> srcDbTbls = dbTbls.stream().filter(e -> srcDbIds.contains(e.getId())).collect(Collectors.toList());
        List<DbTbl> dstDbTbls = dbTbls.stream().filter(e -> dstDbIds.contains(e.getId())).collect(Collectors.toList());
        Map<Long, String> srcDbTblMap = srcDbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, String> dstDbTblMap = dstDbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, Long> dstMhaDbMappingMap = dstMhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

        List<DbReplicationTbl> dbReplicationTblList = dbReplicationTbls.stream()
                .filter(e -> srcMhaDbMappingIds.contains(e.getSrcMhaDbMappingId()) && dstMhaDbMappingIds.contains(e.getDstMhaDbMappingId()))
                .collect(Collectors.toList());
        DcTbl srcDcTbl = dcTbls.stream().filter(e -> e.getId().equals(srcMhatbl.getDcId())).findFirst().get();

        List<ApplierTblV2> curMhaAppliers = applierTbls.stream().
                filter(e -> e.getApplierGroupId().equals(applierGroupTbl.getId())).collect(Collectors.toList());
        for (ApplierTblV2 applierTbl : curMhaAppliers) {
            String resourceIp = resourceTbls.stream().filter(e -> e.getId().equals(applierTbl.getResourceId())).findFirst().map(ResourceTbl::getIp).orElse(StringUtils.EMPTY);
            logger.debug("generate applier: {} for mha: {}", resourceIp, dstMhaTbl.getMhaName());
            Applier applier = new Applier();
            applier.setIp(resourceIp)
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(srcDcTbl.getDcName())
                    .setTargetMhaName(srcMhatbl.getMhaName())
                    .setGtidExecuted(applierGroupTbl.getGtidInit())
                    .setNameFilter(TableNameBuilder.buildNameFilter(srcDbTblMap, srcMhaDbMappingMap, dbReplicationTblList))
                    .setNameMapping(TableNameBuilder.buildNameMapping(srcDbTblMap, srcMhaDbMappingMap, dstDbTblMap, dstMhaDbMappingMap, dbReplicationTblList))
                    .setTargetName(srcMhatbl.getClusterName())
                    .setApplyMode(dstMhaTbl.getApplyMode())
                    .setProperties(getProperties(dbReplicationTblList));
            applier.setTargetRegion(srcDcTbl.getRegionName());
            dbCluster.addApplier(applier);
        }
    }

    private String getProperties(List<DbReplicationTbl> dbReplicationTblList) throws SQLException {
        List<DbReplicationDto> dbReplicationDto = dbReplicationTblList.stream().map(source -> {
            DbReplicationDto target = new DbReplicationDto();
            target.setDbReplicationId(source.getId());
            target.setSrcMhaDbMappingId(source.getSrcMhaDbMappingId());
            target.setSrcLogicTableName(source.getSrcLogicTableName());

            return target;
        }).collect(Collectors.toList());

        DataMediaConfig properties = dataMediaService.generateConfig(dbReplicationDto);
        String propertiesJson = CollectionUtils.isEmpty(properties.getRowsFilters()) &&
                CollectionUtils.isEmpty(properties.getColumnsFilters()) ? null : JsonUtils.toJson(properties);
        return propertiesJson;
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
        if (StringUtils.isNotBlank(proxyIds)) {
            String[] proxyIdArr = proxyIds.split(",");
            for (String idStr : proxyIdArr) {
                Long proxyId = Long.parseLong(idStr);
                proxyTbls.stream().filter(p -> p.getId().equals(proxyId)).findFirst().ifPresent(proxyTbl -> proxyUris.add(proxyTbl.getUri()));
            }
        }
        return proxyUris;
    }
}
