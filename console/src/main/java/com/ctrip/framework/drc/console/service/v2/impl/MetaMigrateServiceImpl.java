package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ColumnsFilterModeEnum;
import com.ctrip.framework.drc.console.enums.DataMediaPairTypeEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.MetaMigrateService;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateMhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/6/5 16:53
 */
@Service
public class MetaMigrateServiceImpl implements MetaMigrateService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MhaTblDao mhaTblDao;
    @Autowired
    private MhaGroupTblDao mhaGroupTblDao;
    @Autowired
    private GroupMappingTblDao groupMappingTblDao;
    @Autowired
    private ClusterTblDao clusterTblDao;
    @Autowired
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierGroupTblDao applierGroupTblDao;
    @Autowired
    private ApplierTblDao applierTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
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
    private DataMediaTblDao dataMediaTblDao;
    @Autowired
    private DataMediaPairTblDao dataMediaPairTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    @Autowired
    private RegionTblDao regionTblDao;
    @Autowired
    private ColumnsFilterTblDao columnsFilterTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private DalServiceImpl dalService;
    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    private final ListeningExecutorService migrateExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(20, "migrateMeta"));
    private static final int MHA_GROUP_SIZE = 2;
    private static final int QUERY_SIZE = 100;
    private static final int BATCH_INSERT_SIZE = 2000;
    private static final String MONITOR_DB = "drcmonitordb\\.delaymonitor";

    @Override
    public int batchInsertRegions(List<String> regionNames) throws Exception {
        List<RegionTbl> regionTbls = regionNames.stream().map(regionName -> {
            RegionTbl regionTbl = new RegionTbl();
            regionTbl.setRegionName(regionName);
            regionTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return regionTbl;
        }).collect(Collectors.toList());
        regionTblDao.batchInsert(regionTbls);
        return regionTbls.size();
    }

    @Override
    public int batchUpdateDcRegions(Map<String, String> dcRegionMap) throws Exception {
        List<DcTbl> dcTbls = dcTblDao.queryAll();
        for (DcTbl dcTbl : dcTbls) {
            if (dcRegionMap.containsKey(dcTbl.getDcName())) {
                dcTbl.setRegionName(dcRegionMap.get(dcTbl.getDcName()));
            }
        }
        dcTblDao.batchUpdate(dcTbls);
        return dcTbls.size();
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateMhaTbl() throws Exception {
        List<MhaTbl> oldMhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaGroupTbl> mhaGroupTbls = mhaGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<GroupMappingTbl> groupMappingTbls = groupMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterTbl> clusterTbls = clusterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterMhaMapTbl> clusterMhaMapTbls = clusterMhaMapTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTblV2> existMhaTbls = mhaTblV2Dao.queryAll();
        List<Long> existMhaTblIds = existMhaTbls.stream().map(MhaTblV2::getId).collect(Collectors.toList());

        Map<Long, MhaGroupTbl> mhaGroupMap = mhaGroupTbls.stream().collect(Collectors.toMap(MhaGroupTbl::getId, Function.identity()));
        Map<Long, Long> groupMap = groupMappingTbls.stream().collect(Collectors.toMap(GroupMappingTbl::getMhaId, GroupMappingTbl::getMhaGroupId, (k1, k2) -> k1));
        Map<Long, Long> clusterMhaMap = clusterMhaMapTbls.stream().collect(Collectors.toMap(ClusterMhaMapTbl::getMhaId, ClusterMhaMapTbl::getClusterId, (k1, k2) -> k1));
        Map<Long, ClusterTbl> clusterMap = clusterTbls.stream().collect(Collectors.toMap(ClusterTbl::getId, Function.identity()));


        List<String> errorMhaNames = new ArrayList<>();
        List<MhaTblV2> newMhaTbls = new ArrayList<>();
        for (MhaTbl oldMhaTbl : oldMhaTbls) {
            if (!groupMap.containsKey(oldMhaTbl.getId()) || !clusterMhaMap.containsKey(oldMhaTbl.getId())) {
                errorMhaNames.add(oldMhaTbl.getMhaName());
                continue;
            }
            long mhaGroupId = groupMap.get(oldMhaTbl.getId());
            long clusterId = clusterMhaMap.get(oldMhaTbl.getId());
            MhaGroupTbl mhaGroupTbl = mhaGroupMap.get(mhaGroupId);
            ClusterTbl clusterTbl = clusterMap.get(clusterId);

            MhaTblV2 newMhaTbl = buildMhaTblV2(oldMhaTbl, mhaGroupTbl, clusterTbl);
            newMhaTbls.add(newMhaTbl);
        }

        List<MhaTblV2> insertMhaTbls = new ArrayList<>();
        List<MhaTblV2> updateMhaTbls = new ArrayList<>();
        for (MhaTblV2 mhaTbl : newMhaTbls) {
            if (existMhaTblIds.contains(mhaTbl.getId())) {
                updateMhaTbls.add(mhaTbl);
            } else {
                insertMhaTbls.add(mhaTbl);
            }
        }

        if (CollectionUtils.isEmpty(errorMhaNames)) {
            if (!CollectionUtils.isEmpty(existMhaTbls)) {
                existMhaTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
                mhaTblV2Dao.batchUpdate(existMhaTbls);
            }
            if (!CollectionUtils.isEmpty(insertMhaTbls)) {
                mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertMhaTbls);
            }
            if (!CollectionUtils.isEmpty(updateMhaTbls)) {
                mhaTblV2Dao.batchUpdate(updateMhaTbls);
            }
        } else {
            throw new IllegalArgumentException(String.format("batchInsert mhaTblV2 fail, error mhaNames: %s", errorMhaNames));
        }

        return new MigrateResult(insertMhaTbls.size(), updateMhaTbls.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateMhaReplication() throws Exception {
        List<MhaGroupTbl> mhaGroupTbls = mhaGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<GroupMappingTbl> groupMappingTbls = groupMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTbl> applierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> existMhaReplications = mhaReplicationTblDao.queryAll();
        if (!CollectionUtils.isEmpty(existMhaReplications)) {
            existMhaReplications.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            mhaReplicationTblDao.batchUpdate(existMhaReplications);
        }

        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getMhaId, ReplicatorGroupTbl::getId));
        Map<Long, List<ApplierGroupTbl>> applierGroupMap = applierGroupTbls.stream().collect(Collectors.groupingBy(ApplierGroupTbl::getReplicatorGroupId));
        Map<Long, List<ApplierTbl>> applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTbl::getApplierGroupId));
        Map<Long, List<Long>> groupMappingMap = groupMappingTbls.stream().collect(Collectors.groupingBy(GroupMappingTbl::getMhaGroupId, Collectors.mapping(GroupMappingTbl::getMhaId, Collectors.toList())));

        List<MhaReplicationTbl> mhaReplicationTbls = new ArrayList<>();
        for (MhaGroupTbl mhaGroupTbl : mhaGroupTbls) {
            List<Long> mhaIds = groupMappingMap.get(mhaGroupTbl.getId());
            if (CollectionUtils.isEmpty(mhaIds) || mhaIds.size() != MHA_GROUP_SIZE) {
                logger.warn("mhaGroupId: {} not match mha replication", mhaGroupTbl.getId());
                continue;
            }
            if (existReplication(mhaIds.get(0), mhaIds.get(1), replicatorGroupMap, applierGroupMap, applierMap)) {
                MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
                mhaReplicationTbl.setSrcMhaId(mhaIds.get(0));
                mhaReplicationTbl.setDstMhaId(mhaIds.get(1));
                mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaReplicationTbls.add(mhaReplicationTbl);
            }
            if (existReplication(mhaIds.get(1), mhaIds.get(0), replicatorGroupMap, applierGroupMap, applierMap)) {
                MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
                mhaReplicationTbl.setSrcMhaId(mhaIds.get(1));
                mhaReplicationTbl.setDstMhaId(mhaIds.get(0));
                mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaReplicationTbls.add(mhaReplicationTbl);
            }
        }

        List<MhaReplicationTbl> insertMhaReplications = new ArrayList<>();
        List<MhaReplicationTbl> updateMhaReplications = new ArrayList<>();

        for (MhaReplicationTbl mhaReplication : mhaReplicationTbls) {
            if (mhaReplicationExist(mhaReplication, existMhaReplications)) {
                updateMhaReplications.add(mhaReplication);
            } else {
                insertMhaReplications.add(mhaReplication);
            }
        }

        if (!CollectionUtils.isEmpty(insertMhaReplications)) {
            mhaReplicationTblDao.batchInsert(insertMhaReplications);
        }
        if (!CollectionUtils.isEmpty(updateMhaReplications)) {
            mhaReplicationTblDao.batchUpdate(updateMhaReplications);
        }
        return new MigrateResult(insertMhaReplications.size(), updateMhaReplications.size());
    }

    private boolean mhaReplicationExist(MhaReplicationTbl mhaReplicationTbl, List<MhaReplicationTbl> existMhaReplications) {
        if (CollectionUtils.isEmpty(existMhaReplications)) {
            return false;
        }
        MhaReplicationTbl existMhaReplication = existMhaReplications.stream()
                .filter(e -> e.getSrcMhaId().equals(mhaReplicationTbl.getSrcMhaId()) && e.getDstMhaId().equals(mhaReplicationTbl.getDstMhaId()))
                .findFirst()
                .orElse(null);
        return existMhaReplication != null;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateApplierGroup() throws Exception {
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> oldApplierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));
        List<ApplierGroupTblV2> existApplierGroups = applierGroupTblV2Dao.queryAll();
        List<Long> existApplierGroupIds = existApplierGroups.stream().map(ApplierGroupTblV2::getId).collect(Collectors.toList());

        List<Long> errorApplierGroupIds = new ArrayList<>();
        List<ApplierGroupTblV2> insertApplierGroups = new ArrayList<>();
        List<ApplierGroupTblV2> updateApplierGroups = new ArrayList<>();
        for (ApplierGroupTbl oldApplierGroupTbl : oldApplierGroupTbls) {
            Long srcMhaId = replicatorGroupMap.get(oldApplierGroupTbl.getReplicatorGroupId());
            if (srcMhaId == null) {
                errorApplierGroupIds.add(oldApplierGroupTbl.getId());
                continue;
            }
            Long dstMhaId = oldApplierGroupTbl.getMhaId();
            Long mhaReplicationId = mhaReplicationTbls.stream()
                    .filter(e -> e.getSrcMhaId().equals(srcMhaId) && e.getDstMhaId().equals(dstMhaId))
                    .findFirst()
                    .map(MhaReplicationTbl::getId)
                    .orElse(-1L);
            ApplierGroupTblV2 newApplierGroupTbl = new ApplierGroupTblV2();
            newApplierGroupTbl.setId(oldApplierGroupTbl.getId());
            newApplierGroupTbl.setMhaReplicationId(mhaReplicationId);
            newApplierGroupTbl.setGtidInit(oldApplierGroupTbl.getGtidExecuted());
            newApplierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());

            if (existApplierGroupIds.contains(oldApplierGroupTbl.getId())) {
                updateApplierGroups.add(newApplierGroupTbl);
            } else {
                insertApplierGroups.add(newApplierGroupTbl);
            }
        }

        if (CollectionUtils.isEmpty(errorApplierGroupIds)) {
            if (!CollectionUtils.isEmpty(existApplierGroups)) {
                existApplierGroups.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
                applierGroupTblV2Dao.batchUpdate(existApplierGroups);
            }
            if (!CollectionUtils.isEmpty(insertApplierGroups)) {
                applierGroupTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertApplierGroups);
            }
            if (!CollectionUtils.isEmpty(updateApplierGroups)) {
                applierGroupTblV2Dao.batchUpdate(updateApplierGroups);
            }
        } else {
            throw new IllegalArgumentException(String.format("batchInsert applierGroup fail, errorApplierGroupIds: %s", errorApplierGroupIds));
        }

        return new MigrateResult(insertApplierGroups.size(), updateApplierGroups.size());
    }

    @Override
    public MigrateResult migrateApplier() throws Exception {
        List<ApplierTbl> oldApplierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTblV2> existAppliers = applierTblV2Dao.queryAll().stream().collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(existAppliers)) {
            existAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            applierTblV2Dao.batchUpdate(existAppliers);
        }
        List<Long> existApplierIds = existAppliers.stream().map(ApplierTblV2::getId).collect(Collectors.toList());

        List<ApplierTblV2> newApplierTbls = oldApplierTbls.stream().map(source -> {
            ApplierTblV2 target = new ApplierTblV2();
            target.setId(source.getId());
            target.setApplierGroupId(source.getApplierGroupId());
            target.setPort(source.getPort());
            target.setResourceId(source.getResourceId());
            target.setMaster(source.getMaster());
            target.setDeleted(source.getDeleted());

            return target;
        }).collect(Collectors.toList());

        List<ApplierTblV2> insertAppliers = new ArrayList<>();
        List<ApplierTblV2> updateAppliers = new ArrayList<>();
        for (ApplierTblV2 applierTbl : newApplierTbls) {
            if (existApplierIds.contains(applierTbl.getId())) {
                updateAppliers.add(applierTbl);
            } else {
                insertAppliers.add(applierTbl);
            }
        }
        if (!CollectionUtils.isEmpty(insertAppliers)) {
            applierTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertAppliers);
        }
        if (!CollectionUtils.isEmpty(updateAppliers)) {
            applierTblV2Dao.batchUpdate(updateAppliers);
        }

        return new MigrateResult(insertAppliers.size(), updateAppliers.size());
    }

    @Override
    public MhaDbMappingResult checkMhaDbMapping() throws Exception {
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<String> existMhaNames = mhaTbls.stream().map(MhaTbl::getMhaName).collect(Collectors.toList());
        List<String> existDbNames = dbTbls.stream().map(DbTbl::getDbName).collect(Collectors.toList());

        Map<String, List<String>> mhaDbMap = getAllDbNames(existMhaNames);
        List<String> allDbNames = new ArrayList<>();
        List<String> allMhaNames = new ArrayList<>();

        mhaDbMap.forEach((mhaName, dbNames) -> {
            allMhaNames.add(mhaName);
            allDbNames.addAll(dbNames);
        });

        List<String> notExistDbNames = new ArrayList<>();
        List<String> notExistMhaNames = new ArrayList<>();
        for (String dbName : allDbNames) {
            if (!existDbNames.contains(dbName)) {
                notExistDbNames.add(dbName);
            }
        }
        for (String mhaName : existMhaNames) {
            if (!allMhaNames.contains(mhaName)) {
                notExistMhaNames.add(mhaName);
            }
        }

        return new MhaDbMappingResult(notExistMhaNames, notExistDbNames);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateMhaDbMappingResult migrateMhaDbMapping() throws Exception {
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<String> existMhaNames = mhaTbls.stream().map(MhaTbl::getMhaName).collect(Collectors.toList());
        List<String> existDbNames = dbTbls.stream().map(DbTbl::getDbName).collect(Collectors.toList());

        Map<String, List<String>> mhaDbMap = getAllDbNames(existMhaNames);
        List<String> allDbNames = new ArrayList<>();
        List<String> allMhaNames = new ArrayList<>();

        mhaDbMap.forEach((mhaName, dbNames) -> {
            allMhaNames.add(mhaName);
            allDbNames.addAll(dbNames);
        });

        List<String> notExistDbNames = new ArrayList<>();
        List<String> notExistMhaNames = new ArrayList<>();
        for (String dbName : allDbNames) {
            if (!existDbNames.contains(dbName)) {
                notExistDbNames.add(dbName);
            }
        }
        for (String mhaName : existMhaNames) {
            if (!allMhaNames.contains(mhaName)) {
                notExistMhaNames.add(mhaName);
            }
        }
        if (!CollectionUtils.isEmpty(notExistDbNames)) {
            throw new IllegalArgumentException(String.format("the following db does not exist: %s", notExistDbNames));
        }

        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        Map<String, Long> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getMhaName, MhaTbl::getId));

        List<MhaDbMappingTbl> mhaDbMappingTblList = new ArrayList<>();
        mhaDbMap.forEach((mhaName, dbNames) -> {
            dbNames.forEach(dbName -> {
                MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
                mhaDbMappingTbl.setMhaId(mhaTblMap.get(mhaName));
                mhaDbMappingTbl.setDbId(dbTblMap.get(dbName));
                mhaDbMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaDbMappingTblList.add(mhaDbMappingTbl);
            });
        });

        int size = mhaDbMappingTblList.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_INSERT_SIZE, size);
            List<MhaDbMappingTbl> subMhaDbMappingList = mhaDbMappingTblList.subList(i, toIndex);
            i += BATCH_INSERT_SIZE;
            mhaDbMappingTblDao.batchInsert(subMhaDbMappingList);
        }
        return new MigrateMhaDbMappingResult(size, notExistMhaNames);
    }

    @Override
    public List<MhaNameFilterVo> checkMhaFilter() throws Exception {
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, Long> applierGroupMap = applierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTbl::getId, ApplierGroupTbl::getReplicatorGroupId));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));
        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));

        List<MhaNameFilterVo> mhaNameFilterVos = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            Set<String> filterTables = new HashSet<>();

            //columnsFilter
            List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByAGroupId(applierGroupTbl.getId(), BooleanEnum.FALSE.getCode());
            filterTables.addAll(dataMediaTbls.stream().map(DataMediaTbl::getFullName).collect(Collectors.toList()));

            //rowsFilter
            List<RowsFilterMappingTbl> rowsFilterMappingTbls = rowsFilterMappingTblDao.queryBy(applierGroupTbl.getId(), ConsumeType.Applier.getCode(), BooleanEnum.FALSE.getCode());
            List<Long> dataMediaIds = rowsFilterMappingTbls.stream().map(RowsFilterMappingTbl::getDataMediaId).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(dataMediaIds)) {
                List<DataMediaTbl> dataMediaTblsList = dataMediaTblDao.queryByIdsAndType(dataMediaIds, DataMediaTypeEnum.ROWS_FILTER.getType(), BooleanEnum.FALSE.getCode());
                filterTables.addAll(dataMediaTblsList.stream().map(DataMediaTbl::getFullName).collect(Collectors.toList()));
            }


            if (filterNeedSplit(applierGroupTbl, filterTables)) {
                MhaNameFilterVo mhaNameFilterVo = new MhaNameFilterVo();
                mhaNameFilterVo.setMhaName(mhaTblMap.get(replicatorGroupMap.get(applierGroupMap.get(applierGroupTbl.getId()))));
                mhaNameFilterVo.setFilterTables(filterTables);
                mhaNameFilterVo.setApplierGroupId(applierGroupTbl.getId());
                mhaNameFilterVo.setNameFilter(applierGroupTbl.getNameFilter());
                mhaNameFilterVos.add(mhaNameFilterVo);
            }
        }
        return mhaNameFilterVos;
    }

    @Override
    public int splitNameFilter(List<NameFilterSplitParam> paramList) throws Exception {
        List<Long> applierGroupIds = paramList.stream().map(NameFilterSplitParam::getApplierGroupId).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> applierGroupIds.contains(e.getId())).collect(Collectors.toList());

        Map<Long, NameFilterSplitParam> paramMap = paramList.stream().collect(Collectors.toMap(NameFilterSplitParam::getApplierGroupId, Function.identity()));

        List<NameFilterSplitParam> errorParamList = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            NameFilterSplitParam splitParam = paramMap.get(applierGroupTbl.getId());
            if (!checkNameFilterContainsSameTables(splitParam.getMhaName(), applierGroupTbl.getNameFilter(), splitParam.getNameFilter())) {
                errorParamList.add(splitParam);
                continue;
            }
            applierGroupTbl.setNameFilter(splitParam.getNameFilter());
        }
        if (!CollectionUtils.isEmpty(errorParamList)) {
            throw new IllegalArgumentException(String.format("errorParamList: %s", errorParamList));
        }
        applierGroupTblDao.batchUpdate(applierGroupTbls);
        return applierGroupTbls.size();
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int migrateColumnsFilter() throws Exception {
        List<ColumnsFilterTbl> oldColumnsFilterTbls = columnsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<Long> existIds = columnFilterTblV2Dao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()))
                .map(ColumnsFilterTblV2::getId).collect(Collectors.toList());

        List<ColumnsFilterTblV2> insertColumnsFilterTbls = new ArrayList<>();
        List<ColumnsFilterTblV2> updateColumnsFilterTbls = new ArrayList<>();
        for (ColumnsFilterTbl oldColumnsFilterTbl : oldColumnsFilterTbls) {
            ColumnsFilterTblV2 newColumnsFilterTbl = new ColumnsFilterTblV2();
            newColumnsFilterTbl.setId(oldColumnsFilterTbl.getId());
            newColumnsFilterTbl.setColumns(oldColumnsFilterTbl.getColumns());
            newColumnsFilterTbl.setMode(ColumnsFilterModeEnum.getCodeByName(oldColumnsFilterTbl.getMode()));
            if (existIds.contains(oldColumnsFilterTbl.getId())) {
                updateColumnsFilterTbls.add(newColumnsFilterTbl);
            } else {
                insertColumnsFilterTbls.add(newColumnsFilterTbl);
            }
        }

        if (!CollectionUtils.isEmpty(updateColumnsFilterTbls)) {
            columnFilterTblV2Dao.batchUpdate(updateColumnsFilterTbls);
        }
        if (!CollectionUtils.isEmpty(insertColumnsFilterTbls)) {
            columnFilterTblV2Dao.batchInsert(insertColumnsFilterTbls);
        }
        return 0;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int migrateDbReplicationTbl() throws Exception {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> oldApplierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTblV2> newApplierGroupTbls = applierGroupTblV2Dao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<Long, Long> newApplierGroupMap = newApplierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTblV2::getMhaReplicationId, ApplierGroupTblV2::getId));
        Map<Long, ApplierGroupTbl> oldApplierGroupMap = oldApplierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTbl::getId, Function.identity()));
        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        List<DbReplicationTbl> dbReplicationTbls = buildDbReplicationTbls(mhaReplicationTbls, mhaDbMappingMap, newApplierGroupMap, oldApplierGroupMap, dbTblMap, mhaTblMap);
        return batchInsertDbReplications(dbReplicationTbls);
    }

    @Override
    public List<String> checkNameMapping() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && StringUtils.isNotBlank(e.getNameMapping()))
                .collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, String> mhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

        Set<String> errorMhaNames = new HashSet<>();
        List<String> mhaNames = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            String nameMappings = applierGroupTbl.getNameMapping();
            String mhaName = mhaMap.get(replicatorGroupMap.get(applierGroupTbl.getId()));
            if (!checkNameMapping(nameMappings)) {
                errorMhaNames.add(mhaName);
                continue;
            }
            mhaNames.add(mhaName);
        }
        if (!CollectionUtils.isEmpty(errorMhaNames)) {
            throw new IllegalArgumentException(String.format("errorMhaNames: %s", errorMhaNames));
        }
        return mhaNames;
    }

    @Override
    public int splitNameFilterWithNameMapping() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && StringUtils.isNotBlank(e.getNameMapping()))
                .collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, String> mhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

        Set<String> errorMhaNames = new HashSet<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            String nameMappings = applierGroupTbl.getNameMapping();
            String mhaName = mhaMap.get(replicatorGroupMap.get(applierGroupTbl.getId()));
            if (!checkNameMapping(nameMappings)) {
                errorMhaNames.add(mhaName);
                continue;
            }

            splitNameFilter(applierGroupTbl, mhaName, errorMhaNames);
        }
        if (!CollectionUtils.isEmpty(errorMhaNames)) {
            throw new IllegalArgumentException(String.format("errorMhaNames: %s", errorMhaNames));
        }

        applierGroupTblDao.batchUpdate(applierGroupTbls);
        return applierGroupTbls.size();
    }

    @Override
    public int migrateMessengerGroup() throws Exception {
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, List<DataMediaPairTbl>> dataMediaPairTblMap = dataMediaPairTbls.stream().collect(Collectors.groupingBy(DataMediaPairTbl::getGroupId));
        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));

        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        List<ListenableFuture<List<DbReplicationTbl>>> futures = new ArrayList<>();
        for (MessengerGroupTbl messengerGroupTbl : messengerGroupTbls) {
            List<DataMediaPairTbl> dataMediaPairTblList = dataMediaPairTblMap.get(messengerGroupTbl.getId());
            dataMediaPairTblList.forEach(dataMediaPairTbl -> {
                ListenableFuture<List<DbReplicationTbl>> future = migrateExecutorService.submit(() ->
                        getMessengerDbReplications(mhaDbMappingMap, dbTblMap, mhaTblMap, dataMediaPairTbl, messengerGroupTbl));
                futures.add(future);
            });
        }

        for (ListenableFuture<List<DbReplicationTbl>> future : futures) {
            try {
                List<DbReplicationTbl> dbReplicationTblList = future.get(3, TimeUnit.SECONDS);
                dbReplicationTbls.addAll(dbReplicationTblList);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("getDbReplications fail, {}", e);
                throw e;
            }
        }

        return batchInsertDbReplications(dbReplicationTbls);
    }

    @Override
    public int migrateMessengerFilter() throws Exception {
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Set<String> propertiesSet = dataMediaPairTbls.stream().map(DataMediaPairTbl::getProperties).collect(Collectors.toSet());
        List<MessengerFilterTbl> messengerFilterTbls = propertiesSet.stream().map(properties -> {
            MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
            messengerFilterTbl.setProperties(properties);
            messengerFilterTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return messengerFilterTbl;
        }).collect(Collectors.toList());

        messengerFilterTblDao.batchInsert(messengerFilterTbls);
        return messengerFilterTbls.size();
    }

    @Override
    public int migrateDbReplicationFilterMapping() throws Exception {
        List<DbReplicationTbl> dbReplications = dbReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTblV2> applierGroupTbls = applierGroupTblV2Dao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = rowsFilterMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ColumnsFilterTbl> columnsFilterTbls = columnsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, MhaDbMappingTbl> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<Long, Long> applierGroupMap = applierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTblV2::getMhaReplicationId, ApplierGroupTblV2::getId));
        Map<Long, DataMediaTbl> dataMediaMap = dataMediaTbls.stream().collect(Collectors.toMap(DataMediaTbl::getId, Function.identity()));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, Long> columnsFilterMap = columnsFilterTbls.stream().collect(Collectors.toMap(ColumnsFilterTbl::getDataMediaId, ColumnsFilterTbl::getId));
        Map<Long, List<RowsFilterMappingTbl>> rowsFilterMappingMap = rowsFilterMappingTbls.stream().collect(Collectors.groupingBy(RowsFilterMappingTbl::getApplierGroupId));
        Map<String, Long> messengerFilterMap = messengerFilterTbls.stream().collect(Collectors.toMap(MessengerFilterTbl::getProperties, MessengerFilterTbl::getId));
        Map<Long, Long> messengerGroupMap = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getMhaId, MessengerGroupTbl::getId));

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = new ArrayList<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplications) {
            MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            MhaDbMappingTbl dstMhaDbMapping = mhaDbMappingMap.get(dbReplicationTbl.getDatachangeLasttime());
            long mhaReplicationId = mhaReplicationTbls.stream()
                    .filter(e -> e.getSrcMhaId().equals(srcMhaDbMapping.getMhaId()) && e.getDstMhaId().equals(dstMhaDbMapping.getMhaId()))
                    .findFirst()
                    .map(MhaReplicationTbl::getId)
                    .get();
            long applierGroupId = applierGroupMap.get(mhaReplicationId);
            String fullName = dbTblMap.get(srcMhaDbMapping.getDbId()) + "\\." + dbReplicationTbl.getSrcLogicTableName();

            long columnsFilterId = -1L;
            long rowsFilterId = -1L;
            long messengerFilterId = -1L;
            switch (DataMediaPairTypeEnum.getByType(dbReplicationTbl.getReplicationType())) {
                case DB_TO_DB:
                    //columnsFilter
                    DataMediaTbl columnsDataMediaTbl = dataMediaTbls.stream()
                            .filter(e -> e.getApplierGroupId().equals(applierGroupId) && e.getFullName().equals(fullName))
                            .findFirst()
                            .orElse(null);
                    if (columnsDataMediaTbl != null) {
                        columnsFilterId = columnsFilterMap.get(columnsDataMediaTbl.getId());
                    }

                    //rowsFilter
                    List<RowsFilterMappingTbl> rowsFilterMappings = rowsFilterMappingMap.get(applierGroupId);
                    if (!CollectionUtils.isEmpty(rowsFilterMappings)) {
                        for (RowsFilterMappingTbl mapping : rowsFilterMappings) {
                            DataMediaTbl rowsDataMediaTbl = dataMediaMap.get(mapping.getDataMediaId());
                            if (rowsDataMediaTbl.getFullName().equals(fullName)) {
                                rowsFilterId = mapping.getRowsFilterId();
                                break;
                            }
                        }
                    }
                    break;
                case DB_TO_MQ:
                    long messengerGroupId = messengerGroupMap.get(srcMhaDbMapping.getMhaId());
                    DataMediaPairTbl dataMediaPairTbl = dataMediaPairTbls.stream()
                            .filter(e -> e.getGroupId().equals(messengerGroupId)
                                    && e.getSrcDataMediaName().equals(fullName)
                                    && e.getDestDataMediaName().equals(dbReplicationTbl.getDstLogicTableName()))
                            .findFirst()
                            .get();
                    messengerFilterId = messengerFilterMap.get(dataMediaPairTbl.getProperties());
                    break;
                default:
                    break;
            }

            if (columnsFilterId != -1L || rowsFilterId != -1L || messengerFilterId != -1L) {
                DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
                dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationTbl.getId());
                dbReplicationFilterMappingTbl.setRowsFilterId(rowsFilterId);
                dbReplicationFilterMappingTbl.setColumnsFilterId(columnsFilterId);
                dbReplicationFilterMappingTbl.setMessengerFilterId(messengerFilterId);
                dbReplicationFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
                dbReplicationFilterMappingTbls.add(dbReplicationFilterMappingTbl);
            }
        }

        dbReplicationFilterMappingTblDao.batchInsert(dbReplicationFilterMappingTbls);
        return dbReplicationFilterMappingTbls.size();
    }

    private List<DbReplicationTbl> buildDbReplicationTbls(List<MhaReplicationTbl> mhaReplicationTbls,
                                                          Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
                                                          Map<Long, Long> newApplierGroupMap,
                                                          Map<Long, ApplierGroupTbl> oldApplierGroupMap,
                                                          Map<String, Long> dbTblMap,
                                                          Map<Long, String> mhaTblMap) throws Exception {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        List<ListenableFuture<List<DbReplicationTbl>>> futures = Lists.newArrayListWithCapacity(mhaReplicationTbls.size());
        for (MhaReplicationTbl mhaReplication : mhaReplicationTbls) {
            ListenableFuture<List<DbReplicationTbl>> future = migrateExecutorService.submit(() ->
                    getDbReplications(mhaDbMappingMap, newApplierGroupMap, oldApplierGroupMap, dbTblMap, mhaTblMap, mhaReplication));
            futures.add(future);
        }

        for (ListenableFuture<List<DbReplicationTbl>> future : futures) {
            try {
                List<DbReplicationTbl> dbReplicationTblList = future.get(3, TimeUnit.SECONDS);
                dbReplicationTbls.addAll(dbReplicationTblList);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("getDbReplications fail, {}", e);
                throw e;
            }
        }
        return dbReplicationTbls;
    }

    private List<DbReplicationTbl> getDbReplications(Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
                                                     Map<Long, Long> newApplierGroupMap,
                                                     Map<Long, ApplierGroupTbl> oldApplierGroupMap,
                                                     Map<String, Long> dbTblMap,
                                                     Map<Long, String> mhaTblMap,
                                                     MhaReplicationTbl mhaReplication) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        long applierGroupId = newApplierGroupMap.get(mhaReplication.getId());
        ApplierGroupTbl applierGroupTbl = oldApplierGroupMap.get(applierGroupId);

        Map<String, String> tableNameMappingMap = new HashMap<>();
        String nameMappings = applierGroupTbl.getNameMapping();
        if (StringUtils.isNotBlank(nameMappings)) {
            List<String> nameMappingList = Lists.newArrayList(nameMappings.split(";"));
            for (String nameMapping : nameMappingList) {
                String[] tables = nameMapping.split(",");
                tableNameMappingMap.put(tables[0].split("\\.")[1], tables[1].split("\\.")[1]);
            }
        }

        String nameFilter = applierGroupTbl.getNameFilter();
        if (StringUtils.isBlank(nameFilter)) {
            List<String> dbNames = queryDbsWithFilter(mhaTblMap.get(mhaReplication.getSrcMhaId()), nameFilter);
            for (String dbName : dbNames) {
                Long dbId = dbTblMap.get(dbName);
                dbReplicationTbls.add(buildDbReplicationTbl(dbId, ".*", "", mhaDbMappingMap, mhaReplication));
            }
        } else {
            List<String> dbFilterList = Lists.newArrayList(nameFilter.split(","));
            for (String dbFilter : dbFilterList) {
                if (MONITOR_DB.equals(dbFilter)) {
                    continue;
                }
                List<String> dbNames = queryDbsWithFilter(mhaTblMap.get(mhaReplication.getSrcMhaId()), dbFilter);
                String[] db = dbFilter.split("\\\\.");
                for (String dbName : dbNames) {
                    String srcTableName = db[1];
                    String dstTableName = tableNameMappingMap.getOrDefault(srcTableName, "");
                    Long dbId = dbTblMap.get(dbName);
                    dbReplicationTbls.add(buildDbReplicationTbl(dbId, srcTableName, dstTableName, mhaDbMappingMap, mhaReplication));
                }
            }
        }
        return dbReplicationTbls;
    }

    private int batchInsertDbReplications(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        int size = dbReplicationTbls.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_INSERT_SIZE, size);
            List<DbReplicationTbl> subMhaDbMappingList = dbReplicationTbls.subList(i, toIndex);
            i += BATCH_INSERT_SIZE;
            dbReplicationTblDao.batchInsert(subMhaDbMappingList);
        }
        return dbReplicationTbls.size();
    }

    private List<DbReplicationTbl> getMessengerDbReplications(Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
                                                              Map<String, Long> dbTblMap,
                                                              Map<Long, String> mhaTblMap,
                                                              DataMediaPairTbl dataMediaPairTbl,
                                                              MessengerGroupTbl messengerGroupTbl) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        String srcDataMediaName = dataMediaPairTbl.getSrcDataMediaName();
        List<String> dbNameFilters = Lists.newArrayList(srcDataMediaName.split(","));
        for (String dbNameFilter : dbNameFilters) {
            List<String> dbNames = queryDbsWithFilter(mhaTblMap.get(messengerGroupTbl.getMhaId()), dbNameFilter);
            String[] tables = dbNameFilter.split("\\\\.");
            for (String dbName : dbNames) {
                long dbId = dbTblMap.get(dbName);
                long srcMhaDbMappingId = mhaDbMappingMap.get(messengerGroupTbl.getMhaId()).stream().filter(e -> e.getDbId().equals(dbId)).findFirst().map(MhaDbMappingTbl::getId).get();
                dbReplicationTbls.add(buildDbReplicationTbl(tables[1], dataMediaPairTbl.getDestDataMediaName(), srcMhaDbMappingId, -1, DataMediaPairTypeEnum.DB_TO_MQ.getType()));
            }
        }
        return dbReplicationTbls;
    }

    private DbReplicationTbl buildDbReplicationTbl(Long dbId,
                                                   String srcTableName,
                                                   String dstTableName,
                                                   Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
                                                   MhaReplicationTbl mhaReplication) {
        long srcMhaDbMappingId = mhaDbMappingMap.get(mhaReplication.getSrcMhaId()).stream().filter(e -> e.getDbId().equals(dbId)).findFirst().map(MhaDbMappingTbl::getId).get();
        long dstMhaDbMappingId = mhaDbMappingMap.get(mhaReplication.getDstMhaId()).stream().filter(e -> e.getDbId().equals(dbId)).findFirst().map(MhaDbMappingTbl::getId).get();
        return buildDbReplicationTbl(srcTableName, dstTableName, srcMhaDbMappingId, dstMhaDbMappingId, DataMediaPairTypeEnum.DB_TO_DB.getType());
    }

    private DbReplicationTbl buildDbReplicationTbl(String srcTableName, String dstTableName, long srcMhaDbMappingId, long dstMhaDbMappingId, int replicationType) {
        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        dbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        dbReplicationTbl.setSrcLogicTableName(srcTableName);
        dbReplicationTbl.setDstLogicTableName(dstTableName);
        dbReplicationTbl.setReplicationType(replicationType);
        dbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return dbReplicationTbl;
    }

    private boolean checkNameMapping(String nameMappings) {
        List<String> nameMappingList = Lists.newArrayList(nameMappings.split(";"));
        for (String nameMapping : nameMappingList) {
            String[] dbs = nameMapping.split(",");
            String srcDb = dbs[0].split("\\.")[0];
            String dstDb = dbs[1].split("\\.")[0];
            if (!srcDb.equals(dstDb)) {
                return false;
            }
        }
        return true;
    }

    private void splitNameFilter(ApplierGroupTbl applierGroupTbl, String mhaName, Set<String> errorMhaNames) {
        List<String> splitDbs = Lists.newArrayList(applierGroupTbl.getNameFilter().split(","));
        if (splitDbs.size() <= 1 || !splitDbs.get(0).equals(MONITOR_DB)) {
            errorMhaNames.add(mhaName);
            return;
        }
        splitDbs.remove(MONITOR_DB);
        List<String> dbs = splitDbs.stream().map(db -> splitNameFilter(db)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbs, ",");
        if (!checkNameFilterContainsSameTables(mhaName, applierGroupTbl.getNameFilter(), newNameFilter)) {
            errorMhaNames.add(mhaName);
            return;
        }

        applierGroupTbl.setNameFilter(newNameFilter);
    }

    private boolean checkNameFilterContainsSameTables(String mhaName, String oldNameFilter, String newNameFilter) {
        List<String> oldTables = queryTablesWithFilter(mhaName, oldNameFilter);
        List<String> newTables = queryTablesWithFilter(mhaName, newNameFilter);

        if (CollectionUtils.isEmpty(oldTables) || CollectionUtils.isEmpty(newTables)) {
            return false;
        }
        Collections.sort(oldTables);
        Collections.sort(newTables);
        return oldTables.equals(newTables);
    }

    private String splitNameFilter(String db) {
        if (!db.contains("(")) {
            return db;
        }
        String[] dbStrings = db.split("\\.");
        String dbName = dbStrings[0];
        String[] preTableStr = dbStrings[1].split("\\(");
        String prefixTable = preTableStr[0];

        String[] sufTableStr = preTableStr[1].split("\\)");
        String tableFilter = sufTableStr[0];

        String suffixTable = "";
        if (sufTableStr.length == 2) {
            suffixTable = sufTableStr[1];
        }
        String finalSuffixTable = suffixTable;

        String[] tables = tableFilter.split("\\|");
        List<String> dbFilters = Arrays.stream(tables).map(table -> buildNameFilter(dbName, table, prefixTable, finalSuffixTable)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbFilters, ",");
        return newNameFilter;
    }

    private String buildNameFilter(String dbName, String table, String prefixTable, String suffixTable) {
        return dbName + "." + prefixTable + table + suffixTable;
    }

    private boolean filterNeedSplit(ApplierGroupTbl applierGroupTbl, Set<String> filterTables) {
        if (CollectionUtils.isEmpty(filterTables)) {
            return false;
        }
        if (StringUtils.isBlank(applierGroupTbl.getNameFilter())) {
            return true;
        }
        List<String> nameFilters = Lists.newArrayList(applierGroupTbl.getNameFilter().split(","));
        return !nameFilters.containsAll(filterTables);
    }

    private List<String> queryDbsWithFilter(String mhaName, String nameFilter) {
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mhaName);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlTables from mha: {},db not exist", mhaName);
            return new ArrayList<>();
        }
        return MySqlUtils.checkDbsWithFilter(endpoint, nameFilter);
    }

    private List<String> queryTablesWithFilter(String mhaName, String nameFilter) {
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mhaName);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlTables from mha: {},db not exist", mhaName);
            return new ArrayList<>();
        }

        List<TableCheckVo> tableCheckVos = MySqlUtils.checkTablesWithFilter(endpoint, nameFilter);
        return tableCheckVos.stream().map(TableCheckVo::getFullName).collect(Collectors.toList());
    }

    private Map<String, List<String>> getAllDbNames(List<String> mhaNames) {
        Map<String, List<String>> mhaDbMap = new HashMap<>();
        int size = mhaNames.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + QUERY_SIZE, size);
            List<String> subMhaNames = mhaNames.subList(i, toIndex);
            i += QUERY_SIZE;
            Map<String, Map<String, List<String>>> dbNameMap = dalService.getDbNames(subMhaNames, EnvUtils.getEnv());
            dbNameMap.forEach((mhaName, dbClusterMap) -> {
                List<String> dbNames = new ArrayList<>();
                dbClusterMap.values().forEach(dbNames::addAll);
                if (!CollectionUtils.isEmpty(dbNames)) {
                    mhaDbMap.put(mhaName, dbNames);
                }
            });
        }

        return mhaDbMap;
    }

    private boolean existReplication(Long srcMhaId, Long dstMhaId, Map<Long, Long> replicatorGroupMap, Map<Long, List<ApplierGroupTbl>> applierGroupMap, Map<Long, List<ApplierTbl>> applierMap) {
        Long replicatorGroupId = replicatorGroupMap.get(srcMhaId);
        if (replicatorGroupId == null) {
            logger.warn("mhaId: {} not match replicatorGroup");
            return false;
        }
        List<ApplierGroupTbl> applierGroupTbls = applierGroupMap.get(replicatorGroupId);
        if (CollectionUtils.isEmpty(applierGroupTbls)) {
            return false;
        }
        ApplierGroupTbl applierGroup = applierGroupTbls.stream().filter(e -> e.getMhaId().equals(dstMhaId)).findFirst().orElse(null);
        if (applierGroup == null) {
            return false;
        }
        return applierMap.containsKey(applierGroup.getId());
    }

    private MhaTblV2 buildMhaTblV2(MhaTbl mhaTbl, MhaGroupTbl mhaGroupTbl, ClusterTbl clusterTbl) {
        MhaTblV2 newMhaTbl = new MhaTblV2();
        newMhaTbl.setId(mhaTbl.getId());
        newMhaTbl.setMhaName(mhaTbl.getMhaName());
        newMhaTbl.setDcId(mhaTbl.getDcId());
        newMhaTbl.setApplyMode(mhaTbl.getApplyMode());
        newMhaTbl.setMonitorSwitch(mhaTbl.getMonitorSwitch());
        newMhaTbl.setBuId(clusterTbl.getBuId());
        newMhaTbl.setClusterName(clusterTbl.getClusterName());
        newMhaTbl.setAppId(clusterTbl.getClusterAppId());
        newMhaTbl.setReadUser(mhaGroupTbl.getReadUser());
        newMhaTbl.setReadPassword(mhaGroupTbl.getReadPassword());
        newMhaTbl.setWriteUser(mhaGroupTbl.getWriteUser());
        newMhaTbl.setWritePassword(mhaGroupTbl.getWritePassword());
        newMhaTbl.setMonitorUser(mhaGroupTbl.getMonitorUser());
        newMhaTbl.setMonitorPassword(mhaGroupTbl.getMonitorPassword());
        newMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return newMhaTbl;
    }
}
