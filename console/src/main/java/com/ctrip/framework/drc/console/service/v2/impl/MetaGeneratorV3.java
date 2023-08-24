package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.v2.ColumnsFilterServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.console.utils.convert.TableNameBuilder;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.base.Joiner;
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
 * V2 fast version
 */
@Service
public class MetaGeneratorV3 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private RowsFilterServiceV2 rowsFilterServiceV2;
    @Autowired
    private ColumnsFilterServiceV2 columnsFilterServiceV2;

    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
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
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Autowired
    private ColumnsFilterTblV2Dao columnsFilterTblV2Dao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;

    private List<BuTbl> buTbls;
    private List<RouteTbl> routeTbls;
    private List<ProxyTbl> proxyTbls;
    private List<DcTbl> dcTbls;
    private List<MhaTblV2> mhaTbls;
    private List<ResourceTbl> resourceTbls;
    private Map<Long, ResourceTbl> resourceTblIdMap;
    private List<MachineTbl> machineTbls;
    private List<ReplicatorGroupTbl> replicatorGroupTbls;
    private List<ApplierGroupTblV2> applierGroupTbls;
    private List<ClusterManagerTbl> clusterManagerTbls;
    private List<ZookeeperTbl> zookeeperTbls;
    private List<ReplicatorTbl> replicatorTbls;
    private List<ApplierTblV2> applierTbls;
    private Map<Long, List<ApplierTblV2>> applierTblsByGroupIdMap;
    private List<MhaReplicationTbl> mhaReplicationTbls;
    private List<MhaDbMappingTbl> mhaDbMappingTbls;
    private Map<Long, List<MhaDbMappingTbl>> mhaDbMappingTblsByMhaIdMap;
    private List<DbReplicationTbl> dbReplicationTbls;
    private List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls;
    private List<ColumnsFilterTblV2> columnsFilterTbls;
    private List<RowsFilterTblV2> rowsFilterTbls;
    private List<DbTbl> dbTbls;
    private List<MessengerFilterTbl> messengerFilterTbls;
    private List<MessengerGroupTbl> messengerGroupTbls;
    private List<MessengerTbl> messengerTbls;
    private Map<Long, List<MessengerTbl>> messengerTblByGroupIdMap;
    private Map<Long, List<DbReplicationFilterMappingTbl>> dbReplicationFilterMappingTblsByDbRplicationIdMap;

    public Drc getDrc() throws Exception {
        Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
        if (publicCloudRegion.contains(consoleConfig.getRegion())) {
            return null;
        }
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Drc drc = new Drc();

        refreshMetaData();
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
                Dc dc = generateDcFrame(drc, dcTbl);
                generateRoute(dc, dcTbl.getId());
                generateClusterManager(dc, dcTbl.getId());
                generateZk(dc, dcTbl.getId());
                generateDbClusters(dc, dcTbl.getId());
            }
        });
    }

    private void refreshMetaData() throws SQLException {
        buTbls = buTblDao.queryAllExist();
        routeTbls = routeTblDao.queryAllExist();
        proxyTbls = proxyTblDao.queryAllExist();
        dcTbls = dcTblDao.queryAllExist();
        mhaTbls = mhaTblDao.queryAllExist();
        resourceTbls = resourceTblDao.queryAllExist();
        machineTbls = machineTblDao.queryAllExist();
        replicatorGroupTbls = replicatorGroupTblDao.queryAllExist();
        applierGroupTbls = applierGroupTblDao.queryAllExist();
        clusterManagerTbls = clusterManagerTblDao.queryAllExist();
        zookeeperTbls = zookeeperTblDao.queryAllExist();
        replicatorTbls = replicatorTblDao.queryAllExist();
        applierTbls = applierTblDao.queryAllExist();
        messengerGroupTbls = messengerGroupTblDao.queryAllExist();
        messengerTbls = messengerTblDao.queryAllExist();

        mhaReplicationTbls = mhaReplicationTblDao.queryAllExist();
        mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist();
        dbReplicationTbls = dbReplicationTblDao.queryAllExist();
        dbTbls = dbTblDao.queryAllExist();
        dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAllExist();
        columnsFilterTbls = columnsFilterTblV2Dao.queryAllExist();
        messengerFilterTbls = messengerFilterTblDao.queryAllExist();
        rowsFilterTbls = rowsFilterTblV2Dao.queryAllExist();

        resourceTblIdMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, Function.identity(), (e1, e2) -> e1));
        mhaDbMappingTblsByMhaIdMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        applierTblsByGroupIdMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));
        messengerTblByGroupIdMap = messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));
        dbReplicationFilterMappingTblsByDbRplicationIdMap = dbReplicationFilterMappingTbls.stream().collect(Collectors.groupingBy(DbReplicationFilterMappingTbl::getDbReplicationId));
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
                ResourceTbl resourceTbl = resourceTblIdMap.get(replicatorTbl.getResourceId());
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
        List<Messenger> messengers = this.generateMessengers(mhaTbl.getId());
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
        List<ApplierTblV2> curMhaAppliers = applierTblsByGroupIdMap.get(applierGroupTbl.getId());
        if (CollectionUtils.isEmpty(curMhaAppliers)) {
            return;
        }
        List<MhaDbMappingTbl> srcMhaDbMappingTbls = mhaDbMappingTblsByMhaIdMap.get(srcMhatbl.getId());
        List<MhaDbMappingTbl> dstMhaDbMappingTbls = mhaDbMappingTblsByMhaIdMap.get(dstMhaTbl.getId());

        Set<Long> srcMhaDbMappingIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toSet());
        Set<Long> dstMhaDbMappingIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toSet());

        Set<Long> srcDbIds = srcMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toSet());
        Set<Long> dstDbIds = dstMhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toSet());
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


        String nameFilter = TableNameBuilder.buildNameFilter(srcDbTblMap, srcMhaDbMappingMap, dbReplicationTblList);
        String nameMapping = TableNameBuilder.buildNameMapping(srcDbTblMap, srcMhaDbMappingMap, dstDbTblMap, dstMhaDbMappingMap, dbReplicationTblList);
        String properties = getProperties(dbReplicationTblList);
        for (ApplierTblV2 applierTbl : curMhaAppliers) {
            String resourceIp = Optional.ofNullable(resourceTblIdMap.get(applierTbl.getResourceId())).map(ResourceTbl::getIp).orElse(StringUtils.EMPTY);
            logger.debug("generate applier: {} for mha: {}", resourceIp, dstMhaTbl.getMhaName());
            Applier applier = new Applier();
            applier.setIp(resourceIp)
                    .setPort(applierTbl.getPort())
                    .setTargetIdc(srcDcTbl.getDcName())
                    .setTargetMhaName(srcMhatbl.getMhaName())
                    .setGtidExecuted(applierGroupTbl.getGtidInit())
                    .setNameFilter(nameFilter)
                    .setNameMapping(nameMapping)
                    .setTargetName(srcMhatbl.getClusterName())
                    .setApplyMode(dstMhaTbl.getApplyMode())
                    .setProperties(properties);
            applier.setTargetRegion(srcDcTbl.getRegionName());
            dbCluster.addApplier(applier);
        }
    }

    private String getProperties(List<DbReplicationTbl> dbReplicationTblList) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationTblList)) {
            return null;
        }
        List<DbReplicationDto> dbReplicationDto = dbReplicationTblList.stream().map(source -> {
            DbReplicationDto target = new DbReplicationDto();
            target.setDbReplicationId(source.getId());
            target.setSrcMhaDbMappingId(source.getSrcMhaDbMappingId());
            target.setSrcLogicTableName(source.getSrcLogicTableName());

            return target;
        }).collect(Collectors.toList());

        DataMediaConfig properties = generateFilters(dbReplicationDto);
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


    private DataMediaConfig generateFilters(List<DbReplicationDto> dbReplicationDtos) throws SQLException {
        // 1. prepare all data

        // 1.1 mha
        Set<Long> srcMhaDbMappingIds = dbReplicationDtos.stream().map(DbReplicationDto::getSrcMhaDbMappingId).collect(Collectors.toSet());
        List<MhaDbMappingTbl> mhaDbMappingTbls = this.mhaDbMappingTbls.stream().filter(e -> srcMhaDbMappingIds.contains(e.getId())).collect(Collectors.toList());
        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

        // 1.2 db
        Set<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toSet());
        List<DbTbl> dbTbls = this.dbTbls.stream().filter(e -> dbIds.contains(e.getId())).collect(Collectors.toList());
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        // 1.3 db replication
        Set<Long> dbReplicationIds = dbReplicationDtos.stream().map(DbReplicationDto::getDbReplicationId).collect(Collectors.toSet());
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = this.dbReplicationFilterMappingTbls.stream().filter(e -> dbReplicationIds.contains(e.getDbReplicationId())).collect(Collectors.toList());
        Map<Long, List<DbReplicationFilterMappingTbl>> dbReplicationMap = dbReplicationFilterMappingTbls
                .stream().collect(Collectors.groupingBy(DbReplicationFilterMappingTbl::getDbReplicationId));

        // 1.4 rows filter
        List<Long> allRowsFilterIds = dbReplicationFilterMappingTbls.stream()
                .map(DbReplicationFilterMappingTbl::getRowsFilterId).filter(NumberUtils::isPositive).collect(Collectors.toList());
        Map<Long, RowsFilterTblV2> rowsFilterMap = rowsFilterTbls.stream().filter(e -> allRowsFilterIds.contains(e.getId()))
                .collect(Collectors.toMap(RowsFilterTblV2::getId, Function.identity()));

        // 1.5 cols filter
        List<Long> allColsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).filter(NumberUtils::isPositive).collect(Collectors.toList());
        Map<Long, ColumnsFilterTblV2> colsFilterMap = columnsFilterTbls.stream().filter(e -> allColsFilterIds.contains(e.getId()))
                .collect(Collectors.toMap(ColumnsFilterTblV2::getId, Function.identity()));

        // 2. build result
        List<ColumnsFilterConfig> columnsFilters = new ArrayList<>();
        List<RowsFilterConfig> rowsFilters = new ArrayList<>();
        for (DbReplicationDto dbReplicationDto : dbReplicationDtos) {
            // table info
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationMap.get(dbReplicationDto.getDbReplicationId());
            if (CollectionUtils.isEmpty(dbReplicationFilterMappings)) {
                continue;
            }
            Long dbId = mhaDbMappingMap.getOrDefault(dbReplicationDto.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationDto.getSrcLogicTableName();

            // rows filter
            List<RowsFilterTblV2> rowsFilterTblList = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getRowsFilterId)
                    .filter(NumberUtils::isPositive)
                    .map(rowsFilterMap::get).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(rowsFilterTblList)) {
                List<RowsFilterConfig> rowsFilterConfigs = rowsFilterServiceV2.generateRowsFiltersConfigFromTbl(tableName, rowsFilterTblList);
                rowsFilters.addAll(rowsFilterConfigs);
            }

            // cols filter
            List<ColumnsFilterTblV2> colsFilterTblList = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getColumnsFilterId)
                    .filter(NumberUtils::isPositive)
                    .map(colsFilterMap::get).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(colsFilterTblList)) {
                List<ColumnsFilterConfig> columnsFilterConfigs = columnsFilterServiceV2.generateColumnsFilterConfigFromTbl(tableName, colsFilterTblList);
                columnsFilters.addAll(columnsFilterConfigs);
            }
        }

        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        dataMediaConfig.setColumnsFilters(columnsFilters);
        dataMediaConfig.setRowsFilters(rowsFilters);
        return dataMediaConfig;
    }


    private List<Messenger> generateMessengers(Long mhaId) throws SQLException {
        List<Messenger> messengers = Lists.newArrayList();
        MessengerGroupTbl messengerGroupTbl = messengerGroupTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).findFirst().orElse(null);
        if (null == messengerGroupTbl) {
            return messengers;
        }

        List<MessengerTbl> messengerTbls = messengerTblByGroupIdMap.getOrDefault(messengerGroupTbl.getId(), Collections.emptyList());
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return messengers;
        }

        MessengerProperties messengerProperties = getMessengerProperties(mhaId);
        String propertiesJson = JsonUtils.toJson(messengerProperties);
        if (CollectionUtils.isEmpty(messengerProperties.getMqConfigs())) {
            logger.info("no mqConfig, should not generate messenger");
            return messengers;
        }
        for (MessengerTbl messengerTbl : messengerTbls) {
            Messenger messenger = new Messenger();
            ResourceTbl resourceTbl = resourceTblIdMap.get(messengerTbl.getResourceId());
            messenger.setIp(resourceTbl.getIp());
            messenger.setPort(messengerTbl.getPort());
            messenger.setNameFilter(messengerProperties.getNameFilter());
            messenger.setGtidExecuted(messengerGroupTbl.getGtidExecuted());
            messenger.setProperties(propertiesJson);
            messengers.add(messenger);
        }
        return messengers;
    }


    private MessengerProperties getMessengerProperties(Long mhaId) throws SQLException {
        MessengerProperties messengerProperties = new MessengerProperties();
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblsByMhaIdMap.get(mhaId);
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> dbReplicationTbls = this.dbReplicationTbls.stream().filter(e -> mhaDbMappingIds.contains(e.getSrcMhaDbMappingId()) && ReplicationTypeEnum.DB_TO_MQ.getType().equals(e.getReplicationType())).collect(Collectors.toList());
        List<DbTbl> dbTbls = this.dbTbls.stream().filter(e -> dbIds.contains(e.getId())).collect(Collectors.toList());
        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        Set<String> srcTables = new HashSet<>();
        List<MqConfig> mqConfigs = new ArrayList<>();
        DataMediaConfig dataMediaConfig = new DataMediaConfig();

        for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationFilterMappingTblsByDbRplicationIdMap.get(dbReplicationTbl.getId());
            Long messengerFilterId = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getMessengerFilterId)
                    .filter(e -> e != null && e > 0L).findFirst().orElse(null);

            MessengerFilterTbl messengerFilterTbl = messengerFilterTbls.stream().filter(e -> e.getId().equals(messengerFilterId)).findFirst().orElse(null);
            if (null == messengerFilterTbl) {
                logger.warn("Messenger Filter is Null, dbReplicationTbl: {}", dbReplicationTbl);
                continue;
            }
            MqConfig mqConfig = JsonUtils.fromJson(messengerFilterTbl.getProperties(), MqConfig.class);

            long dbId = mhaDbMappingMap.getOrDefault(dbReplicationTbl.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationTbl.getSrcLogicTableName();
            srcTables.add(tableName);
            mqConfig.setTable(tableName);
            mqConfig.setTopic(dbReplicationTbl.getDstLogicTableName());
            // processor is null
            mqConfigs.add(mqConfig);
        }

        messengerProperties.setMqConfigs(mqConfigs);
        messengerProperties.setNameFilter(Joiner.on(",").join(srcTables));
        messengerProperties.setDataMediaConfig(dataMediaConfig);
        return messengerProperties;
    }
}
