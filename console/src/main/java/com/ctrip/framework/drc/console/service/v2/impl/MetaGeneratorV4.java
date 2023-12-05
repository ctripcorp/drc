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
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.console.utils.convert.TableNameBuilder;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.xpipe.codec.JsonCodec;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * V3 + thread safe & faster
 */
@Service
public class MetaGeneratorV4 {

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
    private static final ExecutorService executorService = ThreadUtils.newFixedThreadPool(50, "queryAllExist");

    public Drc getDrc() throws Exception {
        Set<String> publicCloudRegion = consoleConfig.getPublicCloudRegion();
        if (publicCloudRegion.contains(consoleConfig.getRegion())) {
            return null;
        }
        // thread safe
        SingleTask task = new SingleTask(rowsFilterServiceV2, columnsFilterServiceV2);
        this.refreshMetaData(task);
        return task.getDrc();
    }

    public static class SingleTask {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        private volatile List<BuTbl> buTbls;
        private volatile List<RouteTbl> routeTbls;
        private volatile List<ProxyTbl> proxyTbls;
        private volatile List<DcTbl> dcTbls;
        private volatile List<MhaTblV2> mhaTbls;
        private volatile List<ResourceTbl> resourceTbls;
        private volatile List<MachineTbl> machineTbls;
        private volatile List<ReplicatorGroupTbl> replicatorGroupTbls;
        private volatile List<ApplierGroupTblV2> applierGroupTbls;
        private volatile List<ClusterManagerTbl> clusterManagerTbls;
        private volatile List<ZookeeperTbl> zookeeperTbls;
        private volatile List<ReplicatorTbl> replicatorTbls;
        private volatile List<ApplierTblV2> applierTbls;
        private volatile List<MhaReplicationTbl> mhaReplicationTbls;
        private volatile List<MhaDbMappingTbl> mhaDbMappingTbls;
        private volatile List<DbReplicationTbl> dbReplicationTbls;
        private volatile List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls;
        private volatile List<ColumnsFilterTblV2> columnsFilterTbls;
        private volatile List<RowsFilterTblV2> rowsFilterTbls;
        private volatile List<DbTbl> dbTbls;
        private volatile List<MessengerFilterTbl> messengerFilterTbls;
        private volatile List<MessengerGroupTbl> messengerGroupTbls;
        private volatile List<MessengerTbl> messengerTbls;


        // index
        private Map<Long, MhaTblV2> mhaTblIdMap;
        private Map<Long, DcTbl> dcTblMap;
        private Map<Long, DbTbl> dbTblMap;
        private Map<Long, ResourceTbl> resourceTblIdMap;
        private Map<Long, List<MhaDbMappingTbl>> mhaDbMappingTblsByMhaIdMap;
        private Map<Long, MhaDbMappingTbl> mhaDbMappingTblsByMappingIdMap;

        private Map<Long, List<ApplierTblV2>> applierTblsByGroupIdMap;
        private Map<Long, List<MessengerTbl>> messengerTblByGroupIdMap;
        private Map<Long, String> mhaDbMappingId2DbNameMap;
        private Map<MultiKey, List<DbReplicationTbl>> dbReplicationByKeyMap;
        private Map<Long, List<DbReplicationFilterMappingTbl>> dbReplicationFilterMappingTblsByDbRplicationIdMap;
        private Map<Long, List<MachineTbl>> machineTblsGroupByMhaIdMap;
        public Map<Long, ReplicatorGroupTbl> replicatorGroupByMhaIdMap;
        public Map<Long, List<ReplicatorTbl>> replicatorsByGroupIdMap;
        public Map<Long, MessengerGroupTbl> messengerGroupByMhaIdMap;
        public Map<Long, ApplierGroupTblV2> applierGroupByMhaReplicationIdMap;
        public Map<Long, List<MhaReplicationTbl>> mhaReplicationGroupByDstMhaIdMap;
        public Map<MultiKey, List<DbReplicationTbl>> dbReplicationByMhaPairMap;
        public Map<Long, RowsFilterTblV2> rowsFilterMap;
        public Map<Long, ColumnsFilterTblV2> colsFilterMap;
        private final RowsFilterServiceV2 rowsFilterServiceV2;
        private final ColumnsFilterServiceV2 columnsFilterServiceV2;

        public SingleTask(RowsFilterServiceV2 rowsFilterServiceV2, ColumnsFilterServiceV2 columnsFilterServiceV2) {
            this.rowsFilterServiceV2 = rowsFilterServiceV2;
            this.columnsFilterServiceV2 = columnsFilterServiceV2;
        }

        public Drc getDrc() throws Exception {
            Drc drc = new Drc();

            for (DcTbl dcTbl : dcTbls) {
                generateDc(drc, dcTbl);
            }
            logger.debug("current DRC: {}", drc);
            return drc;
        }

        private void generateDc(Drc drc, DcTbl dcTbl) throws Exception {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.meta", dcTbl.getDcName(), () -> {
                Dc dc = generateDcFrame(drc, dcTbl);
                generateRoute(dc, dcTbl.getId());
                generateClusterManager(dc, dcTbl.getId());
                generateZk(dc, dcTbl.getId());
                generateDbClusters(dc, dcTbl.getId());
            });
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


                DcTbl srcDcTbl = dcTblMap.get(routeTbl.getSrcDcId());
                DcTbl dstDcTbl = dcTblMap.get(routeTbl.getDstDcId());
                route.setSrcDc(srcDcTbl != null ? srcDcTbl.getDcName() : StringUtils.EMPTY);
                route.setDstDc(dstDcTbl != null ? dstDcTbl.getDcName() : StringUtils.EMPTY);
                route.setSrcRegion(srcDcTbl != null ? srcDcTbl.getRegionName() : StringUtils.EMPTY);
                route.setDstRegion(dstDcTbl != null ? dstDcTbl.getRegionName() : StringUtils.EMPTY);
                logger.info("generate route: {}-{},{}->{}", routeTbl.getRouteOrgId(), routeTbl.getTag(), route.getSrcDc(), route.getDstDc());
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
                zkAddressBuilder.append(resourceIp).append(':').append(zookeeperTbl.getPort());
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
            List<MachineTbl> mhaMachineTblList = machineTblsGroupByMhaIdMap.getOrDefault(mhaTbl.getId(), Collections.emptyList());
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
            ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupByMhaIdMap.get(mhaTbl.getId());
            if (null != replicatorGroupTbl) {
                List<ReplicatorTbl> curMhaReplicators = replicatorsByGroupIdMap.getOrDefault(replicatorGroupTbl.getId(), Collections.emptyList());
                for (ReplicatorTbl replicatorTbl : curMhaReplicators) {
                    ResourceTbl resourceTbl = resourceTblIdMap.get(replicatorTbl.getResourceId());
                    logger.debug("generate replicator: {}:{} for mha: {}", resourceTbl.getIp(), replicatorTbl.getApplierPort(), mhaTbl.getMhaName());
                    Replicator replicator = new Replicator();
                    replicator.setIp(resourceTbl.getIp())
                            .setPort(replicatorTbl.getPort())
                            .setApplierPort(replicatorTbl.getApplierPort())
                            .setExcludedTables(replicatorGroupTbl.getExcludedTables())
                            .setGtidSkip(replicatorTbl.getGtidInit())
                            .setMaster(replicatorTbl.getMaster() == 1);
                    dbCluster.addReplicator(replicator);
                }
            }
        }

        private void generateMessengers(DbCluster dbCluster, MhaTblV2 mhaTbl) {
            List<Messenger> messengers = this.generateMessengers(mhaTbl.getId());
            for (Messenger messenger : messengers) {
                dbCluster.addMessenger(messenger);
            }
        }

        private void generateAppliers(DbCluster dbCluster, MhaTblV2 mhaTbl) throws SQLException {
            List<MhaReplicationTbl> mhaReplicationTblList = mhaReplicationGroupByDstMhaIdMap.getOrDefault(mhaTbl.getId(), Collections.emptyList());

            for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTblList) {
                ApplierGroupTblV2 applierGroupTbl = applierGroupByMhaReplicationIdMap.get(mhaReplicationTbl.getId());
                if (applierGroupTbl == null) {
                    continue;
                }
                MhaTblV2 srcMhaTbl = mhaTblIdMap.get(mhaReplicationTbl.getSrcMhaId());
                generateApplierInstances(dbCluster, srcMhaTbl, mhaTbl, applierGroupTbl);
            }
        }

        private void generateApplierInstances(DbCluster dbCluster, MhaTblV2 srcMhaTbl, MhaTblV2 dstMhaTbl, ApplierGroupTblV2 applierGroupTbl) throws SQLException {
            List<ApplierTblV2> curMhaAppliers = applierTblsByGroupIdMap.get(applierGroupTbl.getId());
            if (CollectionUtils.isEmpty(curMhaAppliers)) {
                return;
            }

            List<DbReplicationTbl> dbReplicationTblList = dbReplicationByMhaPairMap.get(new MultiKey(srcMhaTbl.getId(), dstMhaTbl.getId()));
            DcTbl srcDcTbl = dcTblMap.get(srcMhaTbl.getDcId());

            String nameFilter = TableNameBuilder.buildNameFilter(mhaDbMappingId2DbNameMap, dbReplicationTblList);
            String nameMapping = TableNameBuilder.buildNameMapping(mhaDbMappingId2DbNameMap, dbReplicationTblList);
            String properties = getProperties(dbReplicationTblList);
            for (ApplierTblV2 applierTbl : curMhaAppliers) {
                String resourceIp = Optional.ofNullable(resourceTblIdMap.get(applierTbl.getResourceId())).map(ResourceTbl::getIp).orElse(StringUtils.EMPTY);
                logger.debug("generate applier: {} for mha: {}", resourceIp, dstMhaTbl.getMhaName());
                Applier applier = new Applier();
                applier.setIp(resourceIp)
                        .setPort(applierTbl.getPort())
                        .setTargetIdc(srcDcTbl.getDcName())
                        .setTargetMhaName(srcMhaTbl.getMhaName())
                        .setGtidExecuted(applierGroupTbl.getGtidInit())
                        .setNameFilter(nameFilter)
                        .setNameMapping(nameMapping)
                        .setTargetName(srcMhaTbl.getClusterName())
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
            return CollectionUtils.isEmpty(properties.getRowsFilters()) && CollectionUtils.isEmpty(properties.getColumnsFilters()) ? null : JsonCodec.INSTANCE.encode(properties);
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
            List<ColumnsFilterConfig> columnsFilters = new ArrayList<>();
            List<RowsFilterConfig> rowsFilters = new ArrayList<>();
            for (DbReplicationDto dbReplicationDto : dbReplicationDtos) {
                // table info
                List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationFilterMappingTblsByDbRplicationIdMap.get(dbReplicationDto.getDbReplicationId());
                if (CollectionUtils.isEmpty(dbReplicationFilterMappings)) {
                    continue;
                }
                String dbName = mhaDbMappingId2DbNameMap.getOrDefault(dbReplicationDto.getSrcMhaDbMappingId(), "");
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


        private List<Messenger> generateMessengers(Long mhaId) {
            List<Messenger> messengers = Lists.newArrayList();
            MessengerGroupTbl messengerGroupTbl = messengerGroupByMhaIdMap.get(mhaId);
            if (null == messengerGroupTbl) {
                return messengers;
            }

            List<MessengerTbl> messengerTbls = messengerTblByGroupIdMap.getOrDefault(messengerGroupTbl.getId(), Collections.emptyList());
            if (CollectionUtils.isEmpty(messengerTbls)) {
                return messengers;
            }

            MessengerProperties messengerProperties = getMessengerProperties(mhaId);
            String propertiesJson = JsonCodec.INSTANCE.encode(messengerProperties);
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
                messenger.setApplyMode(ApplyMode.mq.getType());
                messengers.add(messenger);
            }
            return messengers;
        }


        private MessengerProperties getMessengerProperties(Long mhaId) {
            MessengerProperties messengerProperties = new MessengerProperties();
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblsByMhaIdMap.get(mhaId);
            if (mhaDbMappingTbls == null) {
                logger.error("mha{} mhaDbMappingTbls is null", mhaId);
                return messengerProperties;
            }

            List<DbReplicationTbl> dbReplicationTbls = mhaDbMappingTbls.stream()
                    .flatMap(e -> dbReplicationByKeyMap.getOrDefault(new MultiKey(e.getId(), -1L, ReplicationTypeEnum.DB_TO_MQ.getType()), Collections.emptyList()).stream())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            List<DbTbl> dbTbls = mhaDbMappingTbls.stream().map(e -> dbTblMap.get(e.getDbId())).collect(Collectors.toList());
            Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
            Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

            Set<String> srcTables = new HashSet<>();
            List<MqConfig> mqConfigs = new ArrayList<>();

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
            return messengerProperties;
        }


    }

    private void refreshMetaData(SingleTask task) throws Exception {
        List<Future<?>> list = Lists.newArrayList();
        list.add(executorService.submit(() -> task.dbReplicationTbls = dbReplicationTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.buTbls = buTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.routeTbls = routeTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.proxyTbls = proxyTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.dcTbls = dcTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.mhaTbls = mhaTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.resourceTbls = resourceTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.machineTbls = machineTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.replicatorGroupTbls = replicatorGroupTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.applierGroupTbls = applierGroupTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.clusterManagerTbls = clusterManagerTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.zookeeperTbls = zookeeperTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.replicatorTbls = replicatorTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.applierTbls = applierTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.messengerGroupTbls = messengerGroupTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.messengerTbls = messengerTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.mhaReplicationTbls = mhaReplicationTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.dbTbls = dbTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.columnsFilterTbls = columnsFilterTblV2Dao.queryAllExist()));
        list.add(executorService.submit(() -> task.messengerFilterTbls = messengerFilterTblDao.queryAllExist()));
        list.add(executorService.submit(() -> task.rowsFilterTbls = rowsFilterTblV2Dao.queryAllExist()));

        // wait
        for (Future<?> future : list) {
            future.get(5, TimeUnit.SECONDS);
        }

        // index objects
        // map by id
        task.dcTblMap = task.dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, Function.identity()));
        task.resourceTblIdMap = task.resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, Function.identity()));
        task.dbTblMap = task.dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, Function.identity()));
        task.mhaTblIdMap = task.mhaTbls.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));
        task.rowsFilterMap = task.rowsFilterTbls.stream().collect(Collectors.toMap(RowsFilterTblV2::getId, e -> e));
        task.colsFilterMap = task.columnsFilterTbls.stream().collect(Collectors.toMap(ColumnsFilterTblV2::getId, e -> e));
        task.mhaDbMappingTblsByMappingIdMap = task.mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        task.mhaDbMappingId2DbNameMap = task.mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, e -> task.dbTblMap.get(e.getDbId()).getDbName()));

        // map by field
        task.dbReplicationByKeyMap = task.dbReplicationTbls.stream().collect(Collectors.groupingBy(StreamUtils::getKey));
        task.replicatorGroupByMhaIdMap = task.replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getMhaId, e -> e));
        task.messengerGroupByMhaIdMap = task.messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getMhaId, e -> e));
        task.applierGroupByMhaReplicationIdMap = task.applierGroupTbls.stream().filter(e -> e.getMhaReplicationId() != null && e.getMhaReplicationId() > 0).collect(Collectors.toMap(ApplierGroupTblV2::getMhaReplicationId, e -> e));

        // grouping by
        task.dbReplicationByMhaPairMap = task.dbReplicationTbls.stream().filter(e -> ReplicationTypeEnum.DB_TO_DB.getType().equals(e.getReplicationType())).collect(Collectors.groupingBy(e -> {
            MhaDbMappingTbl src = task.mhaDbMappingTblsByMappingIdMap.get(e.getSrcMhaDbMappingId());
            MhaDbMappingTbl dst = task.mhaDbMappingTblsByMappingIdMap.get(e.getDstMhaDbMappingId());
            return new MultiKey(src.getMhaId(), dst.getMhaId());
        }));
        task.mhaReplicationGroupByDstMhaIdMap = task.mhaReplicationTbls.stream().collect(Collectors.groupingBy(MhaReplicationTbl::getDstMhaId));
        task.machineTblsGroupByMhaIdMap = task.machineTbls.stream().collect(Collectors.groupingBy(MachineTbl::getMhaId));
        task.replicatorsByGroupIdMap = task.replicatorTbls.stream().collect(Collectors.groupingBy(ReplicatorTbl::getRelicatorGroupId));
        task.mhaDbMappingTblsByMhaIdMap = task.mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        task.applierTblsByGroupIdMap = task.applierTbls.stream().collect(Collectors.groupingBy(ApplierTblV2::getApplierGroupId));
        task.messengerTblByGroupIdMap = task.messengerTbls.stream().collect(Collectors.groupingBy(MessengerTbl::getMessengerGroupId));
        task.dbReplicationFilterMappingTblsByDbRplicationIdMap = task.dbReplicationFilterMappingTbls.stream().collect(Collectors.groupingBy(DbReplicationFilterMappingTbl::getDbReplicationId));
    }
}
