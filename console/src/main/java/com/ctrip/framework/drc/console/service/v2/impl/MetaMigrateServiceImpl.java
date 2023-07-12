package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.NameFilterSplitParam;
import com.ctrip.framework.drc.console.param.v2.MhaDbMappingMigrateParam;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.MetaMigrateService;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.vo.api.MhaNameFilterVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MhaDbMappingResult;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

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
    private DcTblDao dcTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
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
    private RegionTblDao regionTblDao;
    @Autowired
    private ColumnsFilterTblDao columnsFilterTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private DrcBuildService drcBuildService;
    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private final ListeningExecutorService migrateExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(20, "migrateMeta"));
    private final ListeningExecutorService queryMhaDbExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "queryMhaDb"));
    private static final int MHA_GROUP_SIZE = 2;
    private static final int BATCH_SIZE = 2000;
    private static final int TIME_OUT = 60;
    private static final int QUERY_MYSQL_TIMEOUT = 10;
    private static final String MONITOR_DB = "drcmonitordb\\.delaymonitor";
    private static final String TEST_BU_NAME = "TEST";

    @Override
    public int batchInsertRegions(List<String> regionNames) throws Exception {
        List<RegionTbl> regionTbls = regionNames.stream().map(regionName -> {
            RegionTbl regionTbl = new RegionTbl();
            regionTbl.setRegionName(regionName);
            regionTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return regionTbl;
        }).collect(Collectors.toList());
        logger.info("batchInsert size: {}, regionTbls: {}", regionTbls.size(), regionTbls);
        int[] result = regionTblDao.batchInsert(regionTbls);
        return result.length;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int batchUpdateDcRegions(Map<String, String> dcRegionMap) throws Exception {
        List<DcTbl> dcTbls = dcTblDao.queryAll();
        for (DcTbl dcTbl : dcTbls) {
            if (dcRegionMap.containsKey(dcTbl.getDcName())) {
                dcTbl.setRegionName(dcRegionMap.get(dcTbl.getDcName()));
            }
        }
        logger.info("batchInsert size: {}, dcTbls: {}", dcTbls.size(), dcTbls);
        int[] result = dcTblDao.batchUpdate(dcTbls);

        dcTbls = dcTblDao.queryAll();
        for (DcTbl dcTbl : dcTbls) {
            if (StringUtils.isEmpty(dcTbl.getRegionName())) {
                throw new IllegalArgumentException("regionName is empty");
            }
        }
        return result.length;
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
            if (!clusterMhaMap.containsKey(oldMhaTbl.getId())) {
                errorMhaNames.add(oldMhaTbl.getMhaName());
                continue;
            }
            long mhaGroupId = groupMap.getOrDefault(oldMhaTbl.getId(), 0L);
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

        int insertSize = insertMhaTbls.size();
        int updateSize = updateMhaTbls.size();
        int deleteSize = existMhaTbls.size();
        logger.info("[[migrateMhaTbl]] insertSize: {}, updateSize: {}, deleteSize: {}", insertSize, updateSize, deleteSize);

        if (CollectionUtils.isEmpty(errorMhaNames)) {
            if (!CollectionUtils.isEmpty(existMhaTbls)) {
                existMhaTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
                deleteSize = mhaTblV2Dao.batchUpdate(existMhaTbls).length;
            }
            if (!CollectionUtils.isEmpty(insertMhaTbls)) {
                insertSize = mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertMhaTbls).length;
            }
            if (!CollectionUtils.isEmpty(updateMhaTbls)) {
                updateSize = mhaTblV2Dao.batchUpdate(updateMhaTbls).length;
            }
        } else {
            throw new IllegalArgumentException(String.format("batchInsert mhaTblV2 fail, error mhaNames: %s", errorMhaNames));
        }

        return new MigrateResult(insertSize, updateSize, deleteSize, oldMhaTbls.size());
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

        int deleteSize = existMhaReplications.size();
        logger.info("[[migrateMhaReplication]] insertSize: {}, updateSize: {}, deleteSize: {}", mhaReplicationTbls.size(), 0, deleteSize);
        if (!CollectionUtils.isEmpty(existMhaReplications)) {
            deleteSize = mhaReplicationTblDao.batchDelete(existMhaReplications).length;
        }
        int insertSize = mhaReplicationTblDao.batchInsert(mhaReplicationTbls).length;
        return new MigrateResult(insertSize, 0, deleteSize, mhaReplicationTbls.size());
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

        int insertSize = insertApplierGroups.size();
        int updateSize = updateApplierGroups.size();
        int deleteSize = existApplierGroups.size();
        logger.info("[[migrateApplierGroup]] insertSize: {}, updateSize: {}, deleteSize: {}", insertSize, updateSize, deleteSize);

        if (CollectionUtils.isEmpty(errorApplierGroupIds)) {
            if (!CollectionUtils.isEmpty(existApplierGroups)) {
                existApplierGroups.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
                deleteSize = applierGroupTblV2Dao.batchUpdate(existApplierGroups).length;
            }
            if (!CollectionUtils.isEmpty(insertApplierGroups)) {
                insertSize = applierGroupTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertApplierGroups).length;
            }
            if (!CollectionUtils.isEmpty(updateApplierGroups)) {
                updateSize = applierGroupTblV2Dao.batchUpdate(updateApplierGroups).length;
            }
        } else {
            throw new IllegalArgumentException(String.format("batchInsert applierGroup fail, errorApplierGroupIds: %s", errorApplierGroupIds));
        }

        return new MigrateResult(insertSize, updateSize, deleteSize, oldApplierGroupTbls.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateApplier() throws Exception {
        List<ApplierTbl> oldApplierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTblV2> existAppliers = applierTblV2Dao.queryAll().stream().collect(Collectors.toList());
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

        int insertSize = insertAppliers.size();
        int updateSize = updateAppliers.size();
        int deleteSize = existAppliers.size();
        logger.info("[[migrateApplier]] insertSize: {}, updateSize: {}, deleteSize: {}", insertSize, updateSize, deleteSize);

        if (!CollectionUtils.isEmpty(existAppliers)) {
            existAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            deleteSize = applierTblV2Dao.batchUpdate(existAppliers).length;
        }
        if (!CollectionUtils.isEmpty(insertAppliers)) {
            insertSize = applierTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertAppliers).length;
        }
        if (!CollectionUtils.isEmpty(updateAppliers)) {
            updateSize = applierTblV2Dao.batchUpdate(updateAppliers).length;
        }

        return new MigrateResult(insertSize, updateSize, deleteSize, oldApplierTbls.size());
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
    public MigrateResult migrateMhaDbMapping(List<String> dbBlackList) throws Exception {
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> existMhaDbMappings = mhaDbMappingTblDao.queryAll();
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
        String msg = "";
        if (!CollectionUtils.isEmpty(notExistDbNames)) {
            msg += String.format("the following db does not exist: %s", notExistDbNames);
        }
        if (!CollectionUtils.isEmpty(notExistMhaNames)) {
            msg += String.format("\nthe following mha can not query db: %s", notExistMhaNames);
        }

        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        Map<String, Long> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getMhaName, MhaTbl::getId));

        List<MhaDbMappingTbl> mhaDbMappingTblList = new ArrayList<>();
        mhaDbMap.forEach((mhaName, dbNames) -> {
            for (String dbName : dbNames) {
                if (dbBlackList.contains(dbName)) {
                    continue;
                }
                MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
                mhaDbMappingTbl.setMhaId(mhaTblMap.get(mhaName));
                mhaDbMappingTbl.setDbId(dbTblMap.get(dbName));
                mhaDbMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaDbMappingTblList.add(mhaDbMappingTbl);
            }

        });

        logger.info("[[migrateMhaDbMapping]] insertSize: {}, updateSize: {}, deleteSize: {}", mhaDbMappingTblList.size(), 0, existMhaDbMappings.size());
        int deleteSize = batchDeleteMhaDbMappings(existMhaDbMappings);
        int insertSize = batchInsertMhaDbMappings(mhaDbMappingTblList);
        return new MigrateResult(insertSize, 0, deleteSize, mhaDbMappingTblList.size(), msg);
    }

    @Override
    public List<MhaNameFilterVo> checkMhaFilter() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

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
                mhaNameFilterVo.setMhaName(mhaTblMap.get(replicatorGroupMap.get(applierGroupTbl.getReplicatorGroupId())));
                mhaNameFilterVo.setFilterTables(filterTables);
                mhaNameFilterVo.setApplierGroupId(applierGroupTbl.getId());
                mhaNameFilterVo.setNameFilter(applierGroupTbl.getNameFilter());
                mhaNameFilterVos.add(mhaNameFilterVo);
            }
        }

        logger.info("checkMhaFilter: {}", mhaNameFilterVos);
        return mhaNameFilterVos;
    }

    @Override
    public int splitNameFilter(List<NameFilterSplitParam> paramList) throws Exception {
        logger.info("splitNameFilter params: {}", paramList);
        List<Long> applierGroupIds = paramList.stream().map(NameFilterSplitParam::getApplierGroupId).collect(Collectors.toList());

        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> applierGroupIds.contains(e.getId())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, String> mhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<Long, Long> replicatorMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

        Map<Long, NameFilterSplitParam> paramMap = paramList.stream().collect(Collectors.toMap(NameFilterSplitParam::getApplierGroupId, Function.identity()));

        List<NameFilterSplitParam> errorParamList = new ArrayList<>();
        List<NameFilterSplitParam> errorMhaParamList = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            NameFilterSplitParam splitParam = paramMap.get(applierGroupTbl.getId());
            if (!splitParam.getMhaName().equals(mhaMap.get(replicatorMap.get(applierGroupTbl.getReplicatorGroupId())))) {
                errorMhaParamList.add(splitParam);
            }
            if (!checkNameFilterContainsSameTables(splitParam.getMhaName(), applierGroupTbl.getNameFilter(), splitParam.getNameFilter())) {
                errorParamList.add(splitParam);
                continue;
            }
            applierGroupTbl.setNameFilter(splitParam.getNameFilter());
        }

        if (!CollectionUtils.isEmpty(errorMhaParamList)) {
            throw new IllegalArgumentException(String.format("errorMhaParamList: %s", errorMhaParamList));
        }
        if (!CollectionUtils.isEmpty(errorParamList)) {
            throw new IllegalArgumentException(String.format("errorParamList: %s", errorParamList));
        }
        int result = applierGroupTblDao.batchUpdate(applierGroupTbls).length;
        return result;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateColumnsFilter() throws Exception {
        List<ColumnsFilterTbl> oldColumnsFilterTbls = columnsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ColumnsFilterTblV2> existColumnsFilters = columnFilterTblV2Dao.queryAll();

        Set<ColumnsFilterTblV2> columnsFilterSet = oldColumnsFilterTbls.stream().map(source -> {
            ColumnsFilterTblV2 target = new ColumnsFilterTblV2();
            target.setColumns(source.getColumns());
            target.setMode(ColumnsFilterModeEnum.getCodeByName(source.getMode()));
            target.setDeleted(BooleanEnum.FALSE.getCode());
            return target;
        }).collect(Collectors.toSet());
        List<ColumnsFilterTblV2> insertColumnsFilterTbls = new ArrayList<>(columnsFilterSet);

        int insertSize = insertColumnsFilterTbls.size();
        int deleteSize = existColumnsFilters.size();
        logger.info("[[migrateColumnsFilter]] insertSize: {}, updateSize: {}, deleteSize: {}", insertSize, 0, deleteSize);

        if (!CollectionUtils.isEmpty(existColumnsFilters)) {
            deleteSize = columnFilterTblV2Dao.batchDelete(existColumnsFilters).length;
        }
        if (!CollectionUtils.isEmpty(insertColumnsFilterTbls)) {
            insertSize = columnFilterTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertColumnsFilterTbls).length;
        }
        return new MigrateResult(insertSize, 0, deleteSize, columnsFilterSet.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateRowsFilter() throws Exception {
        List<RowsFilterTbl> oldRowsFilters = rowsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<RowsFilterTblV2> existRowsFilters = rowsFilterTblV2Dao.queryAll();

        Set<RowsFilterTblV2> rowsFilterSet = oldRowsFilters.stream().map(source -> {
            RowsFilterTblV2 target = new RowsFilterTblV2();
            target.setConfigs(source.getConfigs());
            target.setMode(RowsFilterModeEnum.getCodeByName(source.getMode()));
            target.setDeleted(BooleanEnum.FALSE.getCode());
            return target;
        }).collect(Collectors.toSet());
        List<RowsFilterTblV2> insertRowsFilterTbls = new ArrayList<>(rowsFilterSet);

        int insertSize = insertRowsFilterTbls.size();
        int deleteSize = existRowsFilters.size();
        logger.info("[[migrateRowsFilter]] insertSize: {}, updateSize: {}, deleteSize: {}", insertSize, 0, deleteSize);

        if (!CollectionUtils.isEmpty(existRowsFilters)) {
            deleteSize = rowsFilterTblV2Dao.batchDelete(existRowsFilters).length;
        }
        if (!CollectionUtils.isEmpty(insertRowsFilterTbls)) {
            insertSize = rowsFilterTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), insertRowsFilterTbls).length;
        }
        return new MigrateResult(insertSize, 0, deleteSize, rowsFilterSet.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateDbReplicationTbl(List<String> vpcMhaNames) throws Exception {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> oldApplierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTblV2> newApplierGroupTbls = applierGroupTblV2Dao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getMhaReplicationId() > 0L)
                .collect(Collectors.toList());
        List<DbReplicationTbl> existDbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getReplicationType().equals(DataMediaPairTypeEnum.DB_TO_DB.getType()))
                .collect(Collectors.toList());

        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<Long, Long> newApplierGroupMap = newApplierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTblV2::getMhaReplicationId, ApplierGroupTblV2::getId));
        Map<Long, ApplierGroupTbl> oldApplierGroupMap = oldApplierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTbl::getId, Function.identity()));
        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        List<DbReplicationTbl> dbReplicationTbls = buildDbReplicationTbls(mhaReplicationTbls, mhaDbMappingMap, newApplierGroupMap, oldApplierGroupMap, dbTblMap, mhaTblMap, vpcMhaNames);

        int deleteSize = existDbReplicationTbls.size();
        logger.info("[[migrateDbReplicationTbl]] insertSize: {}, updateSize: {}, deleteSize: {}", dbReplicationTbls.size(), 0, deleteSize);
        if (!CollectionUtils.isEmpty(existDbReplicationTbls)) {
            deleteSize = dbReplicationTblDao.batchDelete(existDbReplicationTbls).length;
        }
        int insertSize = batchInsertDbReplications(dbReplicationTbls);
        return new MigrateResult(insertSize, 0, deleteSize, dbReplicationTbls.size());
    }

    @Override
    public List<MhaNameFilterVo> checkNameMapping() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && StringUtils.isNotBlank(e.getNameMapping()))
                .collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, String> mhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));

        List<Long> errorApplierGroupIds = new ArrayList<>();
        List<MhaNameFilterVo> nameFilterVos = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            String nameMappings = applierGroupTbl.getNameMapping();
            String mhaName = mhaMap.get(applierGroupTbl.getMhaId());
            if (!checkNameMapping(nameMappings)) {
                errorApplierGroupIds.add(applierGroupTbl.getId());
                continue;
            }
            MhaNameFilterVo mhaNameFilterVo = new MhaNameFilterVo();
            mhaNameFilterVo.setApplierGroupId(applierGroupTbl.getId());
            mhaNameFilterVo.setMhaName(mhaName);
            mhaNameFilterVo.setNameFilter(applierGroupTbl.getNameFilter());
            nameFilterVos.add(mhaNameFilterVo);
        }
        if (!CollectionUtils.isEmpty(errorApplierGroupIds)) {
            throw new IllegalArgumentException(String.format("errorApplierGroupIds: %s", errorApplierGroupIds));
        }

        logger.info("checkNameMapping: {}", nameFilterVos);
        return nameFilterVos;
    }

    @Override
    public MigrateResult splitNameFilterWithNameMapping() throws Exception {
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && StringUtils.isNotBlank(e.getNameMapping()))
                .collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, String> mhaMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

        List<Long> errorApplierGroupIds = new ArrayList<>();
        for (ApplierGroupTbl applierGroupTbl : applierGroupTbls) {
            String nameMappings = applierGroupTbl.getNameMapping();
            String mhaName = mhaMap.get(replicatorGroupMap.get(applierGroupTbl.getReplicatorGroupId()));
            if (!checkNameMapping(nameMappings) || StringUtils.isBlank(mhaName)) {
                errorApplierGroupIds.add(applierGroupTbl.getId());
                continue;
            }

            splitNameFilter(applierGroupTbl, mhaName, errorApplierGroupIds);
        }
        if (!CollectionUtils.isEmpty(errorApplierGroupIds)) {
            throw new IllegalArgumentException(String.format("errorApplierGroupIds: %s", errorApplierGroupIds));
        }

        int result = applierGroupTblDao.batchUpdate(applierGroupTbls).length;
        return new MigrateResult(0, result, 0, applierGroupTbls.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateMessengerGroup() throws Exception {
        List<Long> messengerGroupIds = messengerTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()))
                .map(MessengerTbl::getMessengerGroupId)
                .collect(Collectors.toList());
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && messengerGroupIds.contains(e.getId()))
                .collect(Collectors.toList());
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaTbl> mhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbReplicationTbl> existDbReplicationTbls = dbReplicationTblDao.queryAll().stream()
                .filter(e -> e.getReplicationType().equals(DataMediaPairTypeEnum.DB_TO_MQ.getType()))
                .collect(Collectors.toList());

        Map<Long, List<DataMediaPairTbl>> dataMediaPairTblMap = dataMediaPairTbls.stream().collect(Collectors.groupingBy(DataMediaPairTbl::getGroupId));
        Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        Map<Long, String> mhaTblMap = mhaTbls.stream().collect(Collectors.toMap(MhaTbl::getId, MhaTbl::getMhaName));


        List<ListenableFuture<Pair<List<DbReplicationTbl>, Pair<Long, Set<String>>>>> futures = new ArrayList<>();
        for (MessengerGroupTbl messengerGroupTbl : messengerGroupTbls) {
            List<DataMediaPairTbl> dataMediaPairTblList = dataMediaPairTblMap.get(messengerGroupTbl.getId());
            if (CollectionUtils.isEmpty(dataMediaPairTblList)) {
                continue;
            }
            dataMediaPairTblList.forEach(dataMediaPairTbl -> {
                ListenableFuture<Pair<List<DbReplicationTbl>, Pair<Long, Set<String>>>> future = migrateExecutorService.submit(() ->
                        getMessengerDbReplications(mhaDbMappingMap, dbTblMap, mhaTblMap, dataMediaPairTbl, messengerGroupTbl));
                futures.add(future);
            });
        }

        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        Map<Long, Set<String>> errorMhaMap = new HashMap<>();
        for (ListenableFuture<Pair<List<DbReplicationTbl>, Pair<Long, Set<String>>>> future : futures) {
            try {
                Pair<List<DbReplicationTbl>, Pair<Long, Set<String>>> resultPair = future.get(TIME_OUT, TimeUnit.SECONDS);
                dbReplicationTbls.addAll(resultPair.getLeft());
                Pair<Long, Set<String>> mhaPair = resultPair.getRight();
                if (!CollectionUtils.isEmpty(mhaPair.getRight())) {
                    errorMhaMap.put(mhaPair.getLeft(), mhaPair.getRight());
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("getDbReplications fail, {}", e);
                throw new RuntimeException(e);
            }
        }

        if (errorMhaMap.size() > 0) {
            throw new IllegalArgumentException(String.format("getDbReplications fail, errorMhaMap: %s", errorMhaMap));
        }
        int deleteSize = existDbReplicationTbls.size();
        logger.info("[[migrateMessengerGroup]] insertSize: {}, updateSize: {}, deleteSize: {}", dbReplicationTbls.size(), 0, deleteSize);
        if (!CollectionUtils.isEmpty(existDbReplicationTbls)) {
            deleteSize = dbReplicationTblDao.batchDelete(existDbReplicationTbls).length;
        }
        int insertSize = batchInsertDbReplications(dbReplicationTbls);
        return new MigrateResult(insertSize, 0, deleteSize, dbReplicationTbls.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateMessengerFilter() throws Exception {
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MessengerFilterTbl> existMessengerFilters = messengerFilterTblDao.queryAll();

        Set<String> propertiesSet = dataMediaPairTbls.stream().map(DataMediaPairTbl::getProperties).collect(Collectors.toSet());
        List<MessengerFilterTbl> messengerFilterTbls = propertiesSet.stream().map(properties -> {
            MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
            messengerFilterTbl.setProperties(properties);
            messengerFilterTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return messengerFilterTbl;
        }).collect(Collectors.toList());

        int deleteSize = existMessengerFilters.size();
        logger.info("[[migrateMessengerFilter]] insertSize: {}, updateSize: {}, deleteSize: {}", messengerFilterTbls.size(), 0, deleteSize);
        if (!CollectionUtils.isEmpty(existMessengerFilters)) {
            deleteSize = messengerFilterTblDao.batchDelete(existMessengerFilters).length;
        }
        int insertSize = messengerFilterTblDao.batchInsert(messengerFilterTbls).length;
        return new MigrateResult(insertSize, 0, deleteSize, messengerFilterTbls.size());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MigrateResult migrateDbReplicationFilterMapping() throws Exception {
        List<DbReplicationTbl> dbReplications = dbReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTblV2> applierGroupTbls = applierGroupTblV2Dao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && e.getMhaReplicationId() > 0L)
                .collect(Collectors.toList());
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = rowsFilterMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ColumnsFilterTbl> columnsFilterTbls = columnsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<RowsFilterTbl> rowsFilterTbls = rowsFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, MhaDbMappingTbl> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<Long, Long> applierGroupMap = applierGroupTbls.stream().collect(Collectors.toMap(ApplierGroupTblV2::getMhaReplicationId, ApplierGroupTblV2::getId));
        Map<Long, DataMediaTbl> dataMediaMap = dataMediaTbls.stream().collect(Collectors.toMap(DataMediaTbl::getId, Function.identity()));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, ColumnsFilterTbl> columnsFilterMap = columnsFilterTbls.stream().collect(Collectors.toMap(ColumnsFilterTbl::getDataMediaId, Function.identity()));
        Map<Long, List<RowsFilterMappingTbl>> rowsFilterMappingMap = rowsFilterMappingTbls.stream().collect(Collectors.groupingBy(RowsFilterMappingTbl::getApplierGroupId));
        Map<String, Long> messengerFilterMap = messengerFilterTbls.stream().collect(Collectors.toMap(MessengerFilterTbl::getProperties, MessengerFilterTbl::getId));
        Map<Long, Long> messengerGroupMap = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getMhaId, MessengerGroupTbl::getId));
        Map<Long, RowsFilterTbl> rowsFilterTblMap = rowsFilterTbls.stream().collect(Collectors.toMap(RowsFilterTbl::getId, Function.identity()));

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = new ArrayList<>();
        Map<Long, Long> errorMessengerMap = new HashMap<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplications) {
            MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            String srcDbName = dbTblMap.get(srcMhaDbMapping.getDbId());
            String srcTableName = dbReplicationTbl.getSrcLogicTableName();

            long columnsFilterId = -1L;
            long rowsFilterId = -1L;
            long messengerFilterId = -1L;
            switch (DataMediaPairTypeEnum.getByType(dbReplicationTbl.getReplicationType())) {
                case DB_TO_DB:
                    MhaDbMappingTbl dstMhaDbMapping = mhaDbMappingMap.get(dbReplicationTbl.getDstMhaDbMappingId());
                    long mhaReplicationId = mhaReplicationTbls.stream()
                            .filter(e -> e.getSrcMhaId().equals(srcMhaDbMapping.getMhaId()) && e.getDstMhaId().equals(dstMhaDbMapping.getMhaId()))
                            .findFirst()
                            .map(MhaReplicationTbl::getId)
                            .get();
                    long applierGroupId = applierGroupMap.get(mhaReplicationId);

                    //columnsFilter
                    DataMediaTbl columnsDataMediaTbl = dataMediaTbls.stream()
                            .filter(e -> e.getApplierGroupId().equals(applierGroupId) && filterMatch(e, srcDbName, srcTableName))
                            .findFirst()
                            .orElse(null);
                    if (columnsDataMediaTbl != null) {
                        ColumnsFilterTbl columnsFilter = columnsFilterMap.get(columnsDataMediaTbl.getId());
                        List<ColumnsFilterTblV2> newColumnsFilters = columnFilterTblV2Dao.queryByColumns(ColumnsFilterModeEnum.getCodeByName(columnsFilter.getMode()), columnsFilter.getColumns());
                        columnsFilterId = newColumnsFilters.stream().filter(e -> e.getColumns().equals(columnsFilter.getColumns())).findFirst().get().getId();
                    }

                    //rowsFilter
                    List<RowsFilterMappingTbl> rowsFilterMappings = rowsFilterMappingMap.get(applierGroupId);
                    if (!CollectionUtils.isEmpty(rowsFilterMappings)) {
                        for (RowsFilterMappingTbl mapping : rowsFilterMappings) {
                            DataMediaTbl rowsDataMediaTbl = dataMediaMap.get(mapping.getDataMediaId());
                            if (filterMatch(rowsDataMediaTbl, srcDbName, srcTableName)) {
                                RowsFilterTbl rowsFilterTbl = rowsFilterTblMap.get(mapping.getRowsFilterId());
                                List<RowsFilterTblV2> newRowsFilters = rowsFilterTblV2Dao.queryByConfigs(RowsFilterModeEnum.getCodeByName(rowsFilterTbl.getMode()), rowsFilterTbl.getConfigs());
                                rowsFilterId = newRowsFilters.stream().filter(e -> e.getConfigs().equals(rowsFilterTbl.getConfigs())).findFirst().get().getId();
                                break;
                            }
                        }
                    }
                    break;
                case DB_TO_MQ:
                    long messengerGroupId = messengerGroupMap.get(srcMhaDbMapping.getMhaId());
                    DataMediaPairTbl dataMediaPairTbl = dataMediaPairTbls.stream()
                            .filter(e -> e.getGroupId().equals(messengerGroupId)
                                    && messengerFilterMatch(e, srcDbName, srcTableName)
                                    && e.getDestDataMediaName().equals(dbReplicationTbl.getDstLogicTableName()))
                            .findFirst()
                            .orElse(null);
                    if (dataMediaPairTbl == null) {
                        errorMessengerMap.put(dbReplicationTbl.getId(), messengerGroupId);
                    } else {
                        messengerFilterId = messengerFilterMap.get(dataMediaPairTbl.getProperties());
                    }
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
        if (errorMessengerMap.size() > 0) {
            throw new RuntimeException(String.format("errorMessengerMap: %s", errorMessengerMap));
        }

        List<DbReplicationFilterMappingTbl> existTbls = dbReplicationFilterMappingTblDao.queryAll();
        int deleteSize = existTbls.size();
        logger.info("[[migrateDbReplicationFilterMapping]] insertSize: {}, updateSize: {}, deleteSize: {}", dbReplicationFilterMappingTbls.size(), 0, deleteSize);
        if (!CollectionUtils.isEmpty(existTbls)) {
            deleteSize = dbReplicationFilterMappingTblDao.batchDelete(existTbls).length;
        }
        int insertSize = dbReplicationFilterMappingTblDao.batchInsert(dbReplicationFilterMappingTbls).length;
        return new MigrateResult(insertSize, 0, deleteSize, dbReplicationFilterMappingTbls.size());
    }

    @Override
    public MigrateResult manualMigrateDbs(List<String> dbs) throws Exception {
        List<String> existDbs = dbTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()))
                .map(DbTbl::getDbName)
                .collect(Collectors.toList());
        List<DbTbl> insertTbls = dbs.stream().filter(db -> !existDbs.contains(db)).map(db -> {
            DbTbl dbTbl = new DbTbl();
            dbTbl.setDbName(db);
            dbTbl.setIsDrc(0);
            dbTbl.setBuName(TEST_BU_NAME);
            dbTbl.setBuCode(TEST_BU_NAME);
            dbTbl.setDbOwner(TEST_BU_NAME);
            dbTbl.setDeleted(0);
            return dbTbl;
        }).collect(Collectors.toList());

        int insertSize = 0;
        if (!CollectionUtils.isEmpty(insertTbls)) {
            insertSize = dbTblDao.batchInsert(insertTbls).length;
        }
        return new MigrateResult(insertSize, 0, 0, insertTbls.size());
    }

    @Override
    public MigrateResult manualMigrateMhaDbMapping(List<String> mhaNames) throws Exception {
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<String> existDbs = dbTbls.stream().map(DbTbl::getDbName).collect(Collectors.toList());
        Map<String, Long> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        Map<String, List<String>> notExistMhaMap = new HashMap<>();
        List<String> notExistMhas = new ArrayList<>();
        Set<String> notExistDbSet = new HashSet<>();
        Map<String, List<String>> mhaDbMap = new HashMap<>();
        List<ListenableFuture<Pair<String, List<String>>>> futures = new ArrayList<>();
        for (String mhaName : mhaNames) {
            ListenableFuture<Pair<String, List<String>>> future = migrateExecutorService.submit(() -> queryMhaDb(mhaName));
            futures.add(future);
        }

        for (ListenableFuture<Pair<String, List<String>>> future : futures) {
            Pair<String, List<String>> mhaPair;
            try {
                mhaPair = future.get(TIME_OUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("query mha db fail, {}", e);
                throw new RuntimeException(e);
            }
            String mhaName = mhaPair.getLeft();
            List<String> dbs = mhaPair.getRight();
            if (CollectionUtils.isEmpty(dbs)) {
                notExistMhas.add(mhaName);
            } else {
                mhaDbMap.put(mhaName, dbs);
                List<String> notExistDbs = dbs.stream().filter(db -> !existDbs.contains(db)).collect(Collectors.toList());
                if (!CollectionUtils.isEmpty(notExistDbs)) {
                    notExistDbSet.addAll(notExistDbs);
                    notExistMhaMap.put(mhaName, notExistDbs);
                }
            }
        }

        if (!CollectionUtils.isEmpty(notExistMhas)) {
            throw new IllegalArgumentException(String.format("notExistMhas: %s", notExistMhas));
        }
        if (!CollectionUtils.isEmpty(notExistDbSet)) {
            throw new IllegalArgumentException(String.format("notExistMhaMap: %s\n notExistDbs: %s", notExistMhaMap, notExistDbSet));
        }

        List<MhaDbMappingTbl> insertTbls = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : mhaDbMap.entrySet()) {
            String mha = entry.getKey();
            List<String> dbs = entry.getValue();
            MhaTbl mhaTbl = mhaTblDao.queryByMhaName(mha, BooleanEnum.FALSE.getCode());
            List<Long> dbIds = mhaDbMappingTblDao.queryByMhaId(mhaTbl.getId()).stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
            for (String db : dbs) {
                long dbId = dbTblMap.get(db);
                if (dbIds.contains(dbId)) {
                    continue;
                }
                MhaDbMappingTbl tbl = new MhaDbMappingTbl();
                tbl.setMhaId(mhaTbl.getId());
                tbl.setDbId(dbId);
                tbl.setDeleted(0);
                insertTbls.add(tbl);
            }
        }

        int insertSize = 0;
        if (!CollectionUtils.isEmpty(insertTbls)) {
            insertSize = mhaDbMappingTblDao.batchInsert(insertTbls).length;
        }
        return new MigrateResult(insertSize, 0, 0, insertTbls.size());
    }

    @Override
    public MigrateResult manualMigrateVPCMhaDbMapping(MhaDbMappingMigrateParam param) throws Exception {
        logger.info("manualMigrateVPCMhaDbMapping param: {}", param);
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(param.getMhaName());
        List<Long> existDbIds = mhaDbMappingTblDao.queryByMhaId(mhaTbl.getId()).stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<String, Long> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        List<MhaDbMappingTbl> insertTbls = new ArrayList<>();
        List<String> notExistDbs = new ArrayList<>();
        for (String dbName : param.getDbs()) {
            if (!dbMap.containsKey(dbName)) {
                notExistDbs.add(dbName);
                continue;
            }
            long dbId = dbMap.get(dbName);
            if (existDbIds.contains(dbId)) {
                continue;
            }

            MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
            mhaDbMappingTbl.setMhaId(mhaTbl.getId());
            mhaDbMappingTbl.setDbId(dbId);
            mhaDbMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            insertTbls.add(mhaDbMappingTbl);
        }

        if (!CollectionUtils.isEmpty(notExistDbs)) {
            throw new RuntimeException(String.format("notExistDbs: %s", notExistDbs));
        }
        int insertSize = 0;
        if (!CollectionUtils.isEmpty(insertTbls)) {
            insertSize = mhaDbMappingTblDao.batchInsert(insertTbls).length;
        }
        return new MigrateResult(insertSize, 0, 0, insertTbls.size());
    }

    @Override
    public List<MhaNameFilterVo> checkVPCMha(List<String> mhaNames) throws Exception {
        List<MhaTblV2> mhaList = mhaTblV2Dao.queryByMhaNames(mhaNames);
        List<Long> mhaIds = mhaList.stream().map(MhaTblV2::getId).collect(Collectors.toList());

        List<ApplierGroupTbl> applierGroups = applierGroupTblDao.queryAll().stream()
                .filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode()) && mhaIds.contains(e.getMhaId()))
                .collect(Collectors.toList());

        Map<Long, String> mhaMap = mhaList.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));
        return applierGroups.stream().map(source -> {
            MhaNameFilterVo target = new MhaNameFilterVo();
            target.setApplierGroupId(source.getId());
            target.setNameFilter(source.getNameFilter());
            target.setMhaName(mhaMap.get(source.getMhaId()));
            return target;
        }).collect(Collectors.toList());
    }

    private boolean filterMatch(DataMediaTbl dataMediaTbl, String db, String table) {
        AviatorRegexFilter regexFilter = new AviatorRegexFilter(dataMediaTbl.getNamespcae());
        return table.equals(dataMediaTbl.getName()) && regexFilter.filter(db);
    }

    private boolean messengerFilterMatch(DataMediaPairTbl dataMediaPairTbl, String db, String table) {
        String[] dbs = dataMediaPairTbl.getSrcDataMediaName().split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
        AviatorRegexFilter regexFilter = new AviatorRegexFilter(dbs[0]);
        String srcTable = dbs[1];
        return table.equals(srcTable) && regexFilter.filter(db);
    }

    private Pair<String, List<String>> queryMhaDb(String mha) {
        List<String> dbs = drcBuildService.queryDbsWithNameFilter(mha, "");
        return Pair.of(mha, dbs);
    }

    private int batchDeleteMhaDbMappings(List<MhaDbMappingTbl> existMhaDbMappingTblList) throws Exception {
        int resultSize = 0;
        if (CollectionUtils.isEmpty(existMhaDbMappingTblList)) {
            return resultSize;
        }
        int size = existMhaDbMappingTblList.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_SIZE, size);
            List<MhaDbMappingTbl> subMhaDbMappingList = existMhaDbMappingTblList.subList(i, toIndex);
            i += BATCH_SIZE;
            resultSize += mhaDbMappingTblDao.batchDelete(subMhaDbMappingList).length;
        }
        return resultSize;
    }

    private int batchInsertMhaDbMappings(List<MhaDbMappingTbl> mhaDbMappingTblList) throws Exception {
        int resultSize = 0;
        int size = mhaDbMappingTblList.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_SIZE, size);
            List<MhaDbMappingTbl> subMhaDbMappingList = mhaDbMappingTblList.subList(i, toIndex);
            i += BATCH_SIZE;
            resultSize += mhaDbMappingTblDao.batchInsert(subMhaDbMappingList).length;
        }
        return resultSize;
    }

    private List<DbReplicationTbl> buildDbReplicationTbls(List<MhaReplicationTbl> mhaReplicationTbls,
                                                          Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
                                                          Map<Long, Long> newApplierGroupMap,
                                                          Map<Long, ApplierGroupTbl> oldApplierGroupMap,
                                                          Map<String, Long> dbTblMap,
                                                          Map<Long, String> mhaTblMap,
                                                          List<String> vpcMhaNames) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        List<ListenableFuture<Pair<List<DbReplicationTbl>, Pair<MhaReplicationTbl, Set<String>>>>> futures = Lists.newArrayListWithCapacity(mhaReplicationTbls.size());
        for (MhaReplicationTbl mhaReplication : mhaReplicationTbls) {
            ListenableFuture<Pair<List<DbReplicationTbl>, Pair<MhaReplicationTbl, Set<String>>>> future = migrateExecutorService.submit(() ->
                    getDbReplications(mhaDbMappingMap, newApplierGroupMap, oldApplierGroupMap, dbTblMap, mhaTblMap, mhaReplication, vpcMhaNames));
            futures.add(future);
        }

        Map<Long, Set<String>> errorMhaReplicationMap = new HashMap<>();
        for (ListenableFuture<Pair<List<DbReplicationTbl>, Pair<MhaReplicationTbl, Set<String>>>> future : futures) {
            try {
                Pair<List<DbReplicationTbl>, Pair<MhaReplicationTbl, Set<String>>> resultPair = future.get(TIME_OUT, TimeUnit.SECONDS);
                dbReplicationTbls.addAll(resultPair.getLeft());
                Pair<MhaReplicationTbl, Set<String>> mhaReplicationPair = resultPair.getRight();
                if (!CollectionUtils.isEmpty(mhaReplicationPair.getRight())) {
                    errorMhaReplicationMap.put(mhaReplicationPair.getLeft().getId(), mhaReplicationPair.getRight());
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("getDbReplications fail, {}", e);
                throw new RuntimeException(e);
            }
        }

        if (errorMhaReplicationMap.size() > 0) {
            throw new IllegalArgumentException(String.format("getDbReplications fail, errorMhaReplicationMap: %s", errorMhaReplicationMap));
        }
        return dbReplicationTbls;
    }

    private Pair<List<DbReplicationTbl>, Pair<MhaReplicationTbl, Set<String>>> getDbReplications(
            Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
            Map<Long, Long> newApplierGroupMap,
            Map<Long, ApplierGroupTbl> oldApplierGroupMap,
            Map<String, Long> dbTblMap,
            Map<Long, String> mhaTblMap,
            MhaReplicationTbl mhaReplication,
            List<String> vpcMhaNames) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        String srcMhaName = mhaTblMap.get(mhaReplication.getSrcMhaId());
        String dstMhaName = mhaTblMap.get(mhaReplication.getDstMhaId());
        if (vpcMhaNames.contains(srcMhaName) && vpcMhaNames.contains(dstMhaName)) {
            return Pair.of(new ArrayList<>(), Pair.of(mhaReplication, new HashSet<>()));
        }

        String mhaName = srcMhaName;
        if (vpcMhaNames.contains(mhaName)) {
            mhaName = dstMhaName;
        }

        long applierGroupId = newApplierGroupMap.get(mhaReplication.getId());
        ApplierGroupTbl applierGroupTbl = oldApplierGroupMap.get(applierGroupId);

        Map<String, String> tableNameMappingMap = new HashMap<>();
        String nameMappings = applierGroupTbl.getNameMapping();
        Set<String> errorDbs = new HashSet<>();
        if (StringUtils.isNotBlank(nameMappings)) {
            List<String> nameMappingList = Lists.newArrayList(nameMappings.split(";"));
            for (String nameMapping : nameMappingList) {
                String[] tables = nameMapping.split(",");
                tableNameMappingMap.put(tables[0].split("\\.")[1], tables[1].split("\\.")[1]);
            }
        }

        String nameFilter = applierGroupTbl.getNameFilter();
        if (StringUtils.isBlank(nameFilter)) {
            List<String> dbNames = drcBuildService.queryDbsWithNameFilter(mhaName, nameFilter);
            if (CollectionUtils.isEmpty(dbNames)) {
                logger.warn("mhaName: {} query db is empty", mhaName);
            }
            for (String dbName : dbNames) {
                Long dbId = dbTblMap.get(dbName);
                try {
                    dbReplicationTbls.add(buildDbReplicationTbl(dbId, ".*", "", mhaDbMappingMap, mhaReplication));
                } catch (Exception e) {
                    errorDbs.add(dbName);
                }
            }
        } else {
            List<String> dbFilterList = Lists.newArrayList(nameFilter.split(","));
            for (String dbFilter : dbFilterList) {
                if (MONITOR_DB.equals(dbFilter)) {
                    continue;
                }

                List<String> dbNames = drcBuildService.queryDbsWithNameFilter(mhaName, dbFilter);
                String[] db = dbFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
                for (String dbName : dbNames) {
                    String srcTableName = "";
                    try {
                        srcTableName = db[1];
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("applierGroupId: %s, errorDbFilter: %s, dbs: %s, %s", applierGroupId, dbFilter, Lists.newArrayList(db), e));
                    }

                    String dstTableName = tableNameMappingMap.getOrDefault(srcTableName, "");
                    Long dbId = dbTblMap.get(dbName);

                    try {
                        dbReplicationTbls.add(buildDbReplicationTbl(dbId, srcTableName, dstTableName, mhaDbMappingMap, mhaReplication));
                    } catch (Exception e) {
                        errorDbs.add(dbName);
                    }
                }
            }
        }

        return Pair.of(dbReplicationTbls, Pair.of(mhaReplication, errorDbs));
    }


    private int batchInsertDbReplications(List<DbReplicationTbl> dbReplicationTbls) throws Exception {
        int result = 0;
        int size = dbReplicationTbls.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_SIZE, size);
            List<DbReplicationTbl> subMhaDbMappingList = dbReplicationTbls.subList(i, toIndex);
            i += BATCH_SIZE;
            result += dbReplicationTblDao.batchInsert(subMhaDbMappingList).length;
        }
        return result;
    }

    private Pair<List<DbReplicationTbl>, Pair<Long, Set<String>>> getMessengerDbReplications(
            Map<Long, List<MhaDbMappingTbl>> mhaDbMappingMap,
            Map<String, Long> dbTblMap,
            Map<Long, String> mhaTblMap,
            DataMediaPairTbl dataMediaPairTbl,
            MessengerGroupTbl messengerGroupTbl) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        String srcDataMediaName = dataMediaPairTbl.getSrcDataMediaName();
        List<String> dbNameFilters = Lists.newArrayList(srcDataMediaName.split(","));
        Set<String> errorDbs = new HashSet<>();
        for (String dbNameFilter : dbNameFilters) {
            List<String> dbNames = new ArrayList<>();
            try {
                dbNames = drcBuildService.queryDbsWithNameFilter(mhaTblMap.get(messengerGroupTbl.getMhaId()), dbNameFilter);
            } catch (Exception e) {
                throw new RuntimeException(String.format("errorMha: %s", mhaTblMap.get(messengerGroupTbl.getMhaId())));
            }

            String[] tables = dbNameFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
            for (String dbName : dbNames) {
                try {
                    long dbId = dbTblMap.get(dbName);
                    long srcMhaDbMappingId = mhaDbMappingMap.get(messengerGroupTbl.getMhaId()).stream().filter(e -> e.getDbId().equals(dbId)).findFirst().map(MhaDbMappingTbl::getId).get();
                    dbReplicationTbls.add(buildDbReplicationTbl(tables[1], dataMediaPairTbl.getDestDataMediaName(), srcMhaDbMappingId, -1, DataMediaPairTypeEnum.DB_TO_MQ.getType()));
                } catch (Exception e) {
                    logger.error("buildDbReplicationTbl fail, dbName: {}, messengerGroupId: {}", dbName, messengerGroupTbl.getId());
                    errorDbs.add(dbName);
                }
            }
        }
        return Pair.of(dbReplicationTbls, Pair.of(messengerGroupTbl.getMhaId(), errorDbs));
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

    private void splitNameFilter(ApplierGroupTbl applierGroupTbl, String mhaName, List<Long> errorApplierGroupIds) {
        List<String> splitDbs = Lists.newArrayList(applierGroupTbl.getNameFilter().split(","));
        if (EnvUtils.pro() && (splitDbs.size() <= 1 || !splitDbs.get(0).equals(MONITOR_DB))) {
            errorApplierGroupIds.add(applierGroupTbl.getId());
            return;
        }
        splitDbs.remove(MONITOR_DB);
        List<String> dbs = splitDbs.stream().map(db -> splitNameFilter(db)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbs, ",");
        if (!checkNameFilterContainsSameTables(mhaName, applierGroupTbl.getNameFilter(), newNameFilter)) {
            errorApplierGroupIds.add(applierGroupTbl.getId());
            return;
        }

        applierGroupTbl.setNameFilter(newNameFilter);
    }

    private boolean checkNameFilterContainsSameTables(String mhaName, String oldNameFilter, String newNameFilter) {
        List<String> oldTables = drcBuildService.queryTablesWithNameFilter(mhaName, oldNameFilter);
        List<String> newTables = drcBuildService.queryTablesWithNameFilter(mhaName, newNameFilter);
        logger.info("split nameFilter mhaName: {}, oldNameFilter: {}, newNameFilter: {}, oldSize: {}, newSize: {}",
                mhaName, oldNameFilter, newNameFilter, oldTables.size(), newTables.size());

        logger.info("mha: {} query tables, oldTables: {}, \n, newTables: {}", mhaName, oldTables, newTables);
        if (CollectionUtils.isEmpty(oldTables) || CollectionUtils.isEmpty(newTables)) {
            return false;
        }
        Collections.sort(oldTables);
        Collections.sort(newTables);
        return oldTables.equals(newTables);
    }

    private String splitNameFilter(String db) {
        String[] dbStrings = db.split("\\.");
        String dbName = dbStrings[0];
        if (!dbStrings[1].contains("(")) {
            return db;
        }
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

    private Map<String, List<String>> getAllDbNames(List<String> mhaNames) {
        Map<String, List<String>> mhaDbMap = new HashMap<>();
        List<ListenableFuture<Pair<String, List<String>>>> futures = new ArrayList<>();
        for (String mhaName : mhaNames) {
            ListenableFuture<Pair<String, List<String>>> future = migrateExecutorService.submit(() -> getMhaDbPair(mhaName));
            futures.add(future);
        }

        for (ListenableFuture<Pair<String, List<String>>> future : futures) {
            try {
                Pair<String, List<String>> resultPair = future.get(QUERY_MYSQL_TIMEOUT, TimeUnit.SECONDS);
                if (!CollectionUtils.isEmpty(resultPair.getRight())) {
                    mhaDbMap.put(resultPair.getLeft(), resultPair.getRight());
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("getDbReplications fail, {}", e);
                throw new RuntimeException(e);
            }
        }
        return mhaDbMap;
    }

    private Pair<String, List<String>> getMhaDbPair(String mhaName) {
        List<String> dbs = new ArrayList<>();
        try {
            dbs = drcBuildService.queryDbsWithNameFilter(mhaName, "");
        } catch (Exception e) {
            logger.error("mha queryDbsWithNameFilter fail, mhaName: {}", mhaName, e);
        }
        return Pair.of(mhaName, dbs);
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
        newMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());

        if (mhaGroupTbl != null) {
            newMhaTbl.setReadUser(mhaGroupTbl.getReadUser());
            newMhaTbl.setReadPassword(mhaGroupTbl.getReadPassword());
            newMhaTbl.setWriteUser(mhaGroupTbl.getWriteUser());
            newMhaTbl.setWritePassword(mhaGroupTbl.getWritePassword());
            newMhaTbl.setMonitorUser(mhaGroupTbl.getMonitorUser());
            newMhaTbl.setMonitorPassword(mhaGroupTbl.getMonitorPassword());
        } else {
            newMhaTbl.setReadUser(monitorTableSourceProvider.getReadUserVal());
            newMhaTbl.setReadPassword(monitorTableSourceProvider.getReadPasswordVal());
            newMhaTbl.setWriteUser(monitorTableSourceProvider.getWriteUserVal());
            newMhaTbl.setWritePassword(monitorTableSourceProvider.getWritePasswordVal());
            newMhaTbl.setMonitorUser(monitorTableSourceProvider.getMonitorUserVal());
            newMhaTbl.setMonitorPassword(monitorTableSourceProvider.getMonitorPasswordVal());
        }

        return newMhaTbl;
    }
}
