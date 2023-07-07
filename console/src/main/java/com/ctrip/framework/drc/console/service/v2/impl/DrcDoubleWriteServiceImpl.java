package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ColumnsFilterModeEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.console.param.v2.MhaDbMappingMigrateParam;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.service.v2.MetaMigrateService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/5 15:01
 */
@Service
public class DrcDoubleWriteServiceImpl implements DrcDoubleWriteService {

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
    private DrcBuildService drcBuildService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private MetaMigrateService metaMigrateService;

    private EventMonitor eventMonitor = DefaultEventMonitorHolder.getInstance();
    private final ListeningExecutorService drcDoubleWriteExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(5, "drcDoubleWrite"));
    private static final int TIME_OUT = 10;
    private static final int GROUP_SIZE = 2;
    private static final String DEFAULT_TABLE_NAME = ".*";

    @Override
    public void buildMha(Long mhaGroupId) throws Exception {
        logger.info("drc double write buildMha, mhaGroupId: {}", mhaGroupId);
        MhaGroupTbl mhaGroup = mhaGroupTblDao.queryByPk(mhaGroupId);
        List<GroupMappingTbl> groupMappings = groupMappingTblDao.queryByMhaGroupIds(Lists.newArrayList(mhaGroupId), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(groupMappings) || groupMappings.size() != GROUP_SIZE) {
            logger.error("groupMapping not exist or contains more than 2 mha, mhaGroupId: {}", mhaGroupId);
            throw ConsoleExceptionUtils.message("groupMapping not exist or contains more than 2 mha");
        }
        long mhaId0 = groupMappings.get(0).getMhaId();
        long mhaId1 = groupMappings.get(1).getMhaId();

        MhaTbl mha0 = mhaTblDao.queryById(mhaId0);
        MhaTbl mha1 = mhaTblDao.queryById(mhaId1);
        if (mha0 == null || mha1 == null) {
            logger.error("mha not exist, mhaGroupId: {}, mhaId0: {}, mhaId1: {}", mhaGroupId, mhaId0, mhaId1);
            throw ConsoleExceptionUtils.message("mha not exist");
        }

        ClusterMhaMapTbl clusterMhaMap0 = clusterMhaMapTblDao.queryByMhaIds(Lists.newArrayList(mhaId0), BooleanEnum.FALSE.getCode()).get(0);
        ClusterMhaMapTbl clusterMhaMap1 = clusterMhaMapTblDao.queryByMhaIds(Lists.newArrayList(mhaId1), BooleanEnum.FALSE.getCode()).get(0);
        ClusterTbl clusterTbl0 = clusterTblDao.queryByPk(clusterMhaMap0.getClusterId());
        ClusterTbl clusterTbl1 = clusterTblDao.queryByPk(clusterMhaMap1.getClusterId());

        MhaTblV2 newMha0 = buildMhaTblV2(mha0, mhaGroup, clusterTbl0);
        MhaTblV2 newMha1 = buildMhaTblV2(mha1, mhaGroup, clusterTbl1);
        insertOrUpdateMha(newMha0);
        insertOrUpdateMha(newMha1);
    }

    @Override
    public void buildApplierGroup(Long applierGroupId) throws Exception {
        logger.info("drc double write buildApplierGroup, applierGroupId: {}", applierGroupId);
        ApplierGroupTbl applierGroupTbl = applierGroupTblDao.queryByPk(applierGroupId);
        ApplierGroupTblV2 existApplierGroup = applierGroupTblV2Dao.queryByPk(applierGroupId);

        ApplierGroupTblV2 newApplierGroupTbl = new ApplierGroupTblV2();
        newApplierGroupTbl.setId(applierGroupTbl.getId());
        newApplierGroupTbl.setMhaReplicationId(-1L);
        newApplierGroupTbl.setGtidInit(applierGroupTbl.getGtidExecuted());
        newApplierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
        if (existApplierGroup != null) {
            applierGroupTblV2Dao.update(newApplierGroupTbl);
        } else {
            applierGroupTblV2Dao.insert(new DalHints().enableIdentityInsert(), newApplierGroupTbl);
        }

    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildAppliers(Long applierGroupId) throws Exception {
        logger.info("drc double write buildAppliers, applierGroupId: {}", applierGroupId);
        List<ApplierTbl> applierTbls = applierTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
        List<ApplierTblV2> existApplierTbls = applierTblV2Dao.queryByApplierGroupId(applierGroupId);
        List<Long> existApplierIds = existApplierTbls.stream().map(ApplierTblV2::getId).collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(existApplierTbls)) {
            existApplierTbls.forEach(e -> {
                e.setDeleted(BooleanEnum.TRUE.getCode());
            });
            applierTblV2Dao.batchUpdate(existApplierTbls);
        }

        if (!CollectionUtils.isEmpty(applierTbls)) {
            for (ApplierTbl applierTbl : applierTbls) {
                ApplierTblV2 applierTblV2 = buildApplier(applierTbl);
                if (existApplierIds.contains(applierTblV2.getId())) {
                    applierTblV2Dao.update(applierTblV2);
                } else {
                    applierTblV2Dao.insert(new DalHints().enableIdentityInsert(), applierTblV2);
                }
            }
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMhaAndDbReplication(Long applierGroupId) throws Exception {
        logger.info("drc double write buildMhaReplication, applierGroupId: {}", applierGroupId);
        List<ApplierTbl> applierTbls = applierTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(applierTbls)) {
            logger.info("applierTbls is empty, applierGroupId: {}", applierGroupId);
            return;
        }

        ApplierGroupTbl applierGroupTbl = applierGroupTblDao.queryByPk(applierGroupId);
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByPk(applierGroupTbl.getReplicatorGroupId());
        MhaTblV2 srcMha = mhaTblV2Dao.queryByPk(replicatorGroupTbl.getMhaId());
        MhaTblV2 dstMha = mhaTblV2Dao.queryByPk(applierGroupTbl.getMhaId());

        long mhaReplicationId = insertOrUpdateMhaReplication(srcMha.getId(), applierGroupTbl.getMhaId());
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByPk(applierGroupId);
        applierGroupTblV2.setMhaReplicationId(mhaReplicationId);
        applierGroupTblV2Dao.update(applierGroupTblV2);

        List<String> dbList = insertMhaDbMappings(applierGroupTbl, srcMha, dstMha);
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());

        insertDbReplications(applierGroupTbl,srcMhaDbMappings, dstMhaDbMappings, dbList);
        insertDbReplicationFilterMapping(applierGroupTbl, srcMhaDbMappings, dstMhaDbMappings, dbList);
    }

    private void insertDbReplicationFilterMapping(ApplierGroupTbl applierGroupTbl, List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings, List<String> dbList) throws Exception {
        List<DbReplicationTbl> dbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByAGroupId(applierGroupTbl.getId(), BooleanEnum.FALSE.getCode());
        List<RowsFilterMappingTbl> rowsFilterMappings = rowsFilterMappingTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbList);
        Map<Long, String> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, MhaDbMappingTbl> srcMhaMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = new ArrayList<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplications) {
            long columnsFilterId = -1L;
            long rowsFilterId = -1L;

            MhaDbMappingTbl srcMhaDbMapping = srcMhaMappingMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            String srcDbName = dbMap.get(srcMhaDbMapping.getDbId());
            String srcTableName = dbReplicationTbl.getSrcLogicTableName();

            //columnsFilter
            DataMediaTbl columnsDataMediaTbl = dataMediaTbls.stream()
                    .filter(e -> filterMatch(e, srcDbName, srcTableName))
                    .findFirst()
                    .orElse(null);
            if (columnsDataMediaTbl != null) {
                ColumnsFilterTbl columnsFilter = columnsFilterTblDao.queryByDataMediaId(columnsDataMediaTbl.getId(), BooleanEnum.FALSE.getCode());
                List<ColumnsFilterTblV2> newColumnsFilters = columnFilterTblV2Dao.queryByColumns(ColumnsFilterModeEnum.getCodeByName(columnsFilter.getMode()), columnsFilter.getColumns());
                columnsFilterId = newColumnsFilters.stream().filter(e -> e.getColumns().equals(columnsFilter.getColumns())).findFirst().get().getId();
            }

            //rowsFilter
            for (RowsFilterMappingTbl mapping : rowsFilterMappings) {
                DataMediaTbl rowsDataMediaTbl = dataMediaTblDao.queryById(mapping.getDataMediaId());
                if (filterMatch(rowsDataMediaTbl, srcDbName, srcTableName)) {
                    RowsFilterTbl rowsFilterTbl = rowsFilterTblDao.queryById(mapping.getRowsFilterId(), BooleanEnum.FALSE.getCode());
                    List<RowsFilterTblV2> newRowsFilters = rowsFilterTblV2Dao.queryByConfigs(RowsFilterModeEnum.getCodeByName(rowsFilterTbl.getMode()), rowsFilterTbl.getConfigs());
                    rowsFilterId = newRowsFilters.stream().filter(e -> e.getConfigs().equals(rowsFilterTbl.getConfigs())).findFirst().get().getId();
                    break;
                }
            }

            if (columnsFilterId != -1L || rowsFilterId != -1L) {
                dbReplicationFilterMappingTbls.add(buildDbReplicationFilterMappingTbl(dbReplicationTbl.getId(), rowsFilterId, columnsFilterId, -1L));
            }
        }

        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTblDao.batchInsert(dbReplicationFilterMappingTbls);
        }
    }

    private DbReplicationFilterMappingTbl buildDbReplicationFilterMappingTbl(long dbReplicationId, long rowsFilterId, long columnsFilterId, long messengerFilterId) {
        DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
        dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationId);
        dbReplicationFilterMappingTbl.setRowsFilterId(rowsFilterId);
        dbReplicationFilterMappingTbl.setColumnsFilterId(columnsFilterId);
        dbReplicationFilterMappingTbl.setMessengerFilterId(messengerFilterId);
        dbReplicationFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return dbReplicationFilterMappingTbl;
    }

    private boolean filterMatch(DataMediaTbl dataMediaTbl, String db, String table) {
        AviatorRegexFilter regexFilter = new AviatorRegexFilter(dataMediaTbl.getNamespcae());
        return table.equals(dataMediaTbl.getName()) && regexFilter.filter(db);
    }

    private Long insertOrUpdateMhaReplication(Long srcMhaId, Long dstMhaId) throws Exception{
        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setSrcMhaId(srcMhaId);
        mhaReplicationTbl.setDstMhaId(dstMhaId);
        mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());

        Long mhaReplicationId = null;
        MhaReplicationTbl existMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId);
        if (existMhaReplication != null) {
            mhaReplicationId = existMhaReplication.getId();
            mhaReplicationTblDao.update(mhaReplicationTbl);
        } else {
            mhaReplicationId = mhaReplicationTblDao.insertWithReturnId(mhaReplicationTbl);
        }
        return mhaReplicationId;
    }

    private void insertDbReplications(ApplierGroupTbl applierGroupTbl, List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings, List<String> dbList) throws Exception {
        //delete before insert
        List<DbReplicationTbl> existDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        deleteDbReplications(existDbReplications);

        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));
        Map<Long, Long> dstMhaDbMappingMap = dstMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbList);
        Map<String, Long> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        String nameFilter = applierGroupTbl.getNameFilter();
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        if (StringUtils.isBlank(nameFilter)) {
            dbReplicationTbls.addAll(buildDbReplications(dbList, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, DEFAULT_TABLE_NAME));
        } else {
            List<String> dbFilterList = Lists.newArrayList(nameFilter.split(","));
            for (String dbFilter : dbFilterList) {
                String[] dbFullNames = dbFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
                String srcTableName = dbFullNames[1];
                AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(dbFilter);
                List<String> dbNames = dbList.stream().filter(db -> aviatorRegexFilter.filter(db)).collect(Collectors.toList());
                dbReplicationTbls.addAll(buildDbReplications(dbNames, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, srcTableName));
            }
        }

        dbReplicationTblDao.batchInsert(dbReplicationTbls);
    }

    private List<DbReplicationTbl> getExistDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());
        return existDbReplications;
    }

    private List<String> insertMhaDbMappings(ApplierGroupTbl applierGroupTbl, MhaTblV2 srcMha, MhaTblV2 dstMha) throws Exception {
        List<String> vpcMhaNames = defaultConsoleConfig.getVpcMhaNames();
        String nameFilter = applierGroupTbl.getNameFilter();
        if (vpcMhaNames.contains(srcMha.getMhaName()) || vpcMhaNames.contains(dstMha.getMhaName())) {
            return insertVpcMhaDbMappings(srcMha, dstMha, vpcMhaNames, nameFilter);
        } else {
            return insertMhaDbMappings(srcMha, dstMha, nameFilter);
        }
    }

    private List<String> insertMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, String nameFilter) throws Exception {
        List<String> srcDbList = drcBuildService.queryDbsWithNameFilter(srcMha.getMhaName(), nameFilter);
        List<String> dstDbList = drcBuildService.queryDbsWithNameFilter(dstMha.getMhaName(), nameFilter);
        if (!checkDbIsSame(srcDbList, dstDbList)) {
            logger.error("drc double write insertMhaDbMappings srcDb dstDb is not same, srcDbList: {}, dstDbList: {}", srcDbList, dstDbList);
            throw ConsoleExceptionUtils.message("srcMha dstMha contains different dbs");
        }

        MigrateResult migrateResult = metaMigrateService.manualMigrateDbs(srcDbList);
        logger.info("drc double write insertMhaDbMapping dbList: {}, migrateResult: {}", srcDbList, migrateResult);

        //insertMhaDbMappings
        metaMigrateService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam(srcMha.getMhaName(), srcDbList));
        metaMigrateService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam(dstMha.getMhaName(), dstDbList));

        return srcDbList;
    }

    private boolean checkDbIsSame(List<String> srcDbList, List<String> dstDbList) {
        if (CollectionUtils.isEmpty(srcDbList) || CollectionUtils.isEmpty(dstDbList)) {
            return false;
        }
        if (srcDbList.size() != dstDbList.size()) {
            return false;
        }
        Collections.sort(srcDbList);
        Collections.sort(dstDbList);
        return srcDbList.equals(dstDbList);
    }


    private List<String> insertVpcMhaDbMappings(MhaTblV2 srcMha, MhaTblV2 dstMha, List<String> vpcMhaNames, String nameFilter) throws Exception {
        String mhaName = vpcMhaNames.contains(srcMha.getMhaName()) ? dstMha.getMhaName() : srcMha.getMhaName();
        List<String> dbList = drcBuildService.queryDbsWithNameFilter(mhaName, nameFilter);
        MigrateResult migrateResult = metaMigrateService.manualMigrateDbs(dbList);
        logger.info("drc double write insertVpcMhaDbMappings dbList: {}, migrateResult: {}", dbList, migrateResult);

        //insertMhaDbMappings
        metaMigrateService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam(srcMha.getMhaName(), dbList));
        metaMigrateService.manualMigrateVPCMhaDbMapping(new MhaDbMappingMigrateParam(dstMha.getMhaName(), dbList));

        return dbList;
    }


    private void deleteDbReplications(List<DbReplicationTbl> existDbReplications) throws Exception {
        if (CollectionUtils.isEmpty(existDbReplications)) {
            return;
        }
        existDbReplications.forEach(e -> {
            e.setDeleted(BooleanEnum.TRUE.getCode());
        });
        dbReplicationTblDao.batchUpdate(existDbReplications);
        deleteDbReplicationFilterMappings(existDbReplications);
    }

    private void deleteDbReplicationFilterMappings(List<DbReplicationTbl> existDbReplications) throws Exception {
        List<Long> dbReplicationIds = existDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        List<DbReplicationFilterMappingTbl> existDbReplicationFilterMappings = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(existDbReplicationFilterMappings)) {
            return;
        }
        existDbReplicationFilterMappings.forEach(e -> {
            e.setDeleted(BooleanEnum.TRUE.getCode());
        });
        dbReplicationFilterMappingTblDao.batchUpdate(existDbReplicationFilterMappings);
    }



    private ApplierTblV2 buildApplier(ApplierTbl applierTbl) {
        ApplierTblV2 applierTblV2 = new ApplierTblV2();
        applierTblV2.setId(applierTbl.getId());
        applierTblV2.setApplierGroupId(applierTbl.getApplierGroupId());
        applierTblV2.setPort(applierTbl.getPort());
        applierTblV2.setResourceId(applierTbl.getResourceId());
        applierTblV2.setMaster(applierTbl.getMaster());
        applierTblV2.setDeleted(applierTbl.getDeleted());

        return applierTblV2;
    }

    private void insertOrUpdateMha(MhaTblV2 mhaTbl) throws Exception {
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByPk(mhaTbl.getId());
        if (oldMhaTbl != null) {
            mhaTblV2Dao.update(mhaTbl);
        } else {
            mhaTblV2Dao.insert(new DalHints().enableIdentityInsert(), mhaTbl);
        }
    }

    private MhaTblV2 buildMhaTblV2(MhaTbl mhaTbl, MhaGroupTbl mhaGroupTbl, ClusterTbl clusterTbl) {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setId(mhaTbl.getId());
        mhaTblV2.setMhaName(mhaTbl.getMhaName());
        mhaTblV2.setDcId(mhaTbl.getDcId());
        mhaTblV2.setApplyMode(mhaTbl.getApplyMode());
        mhaTblV2.setMonitorSwitch(mhaTbl.getMonitorSwitch());
        mhaTblV2.setBuId(clusterTbl.getBuId());
        mhaTblV2.setClusterName(clusterTbl.getClusterName());
        mhaTblV2.setAppId(clusterTbl.getClusterAppId());
        mhaTblV2.setDeleted(BooleanEnum.FALSE.getCode());

        mhaTblV2.setReadUser(mhaGroupTbl.getReadUser());
        mhaTblV2.setReadPassword(mhaGroupTbl.getReadPassword());
        mhaTblV2.setWriteUser(mhaGroupTbl.getWriteUser());
        mhaTblV2.setWritePassword(mhaGroupTbl.getWritePassword());
        mhaTblV2.setMonitorUser(mhaGroupTbl.getMonitorUser());
        mhaTblV2.setMonitorPassword(mhaGroupTbl.getMonitorPassword());

        return mhaTblV2;
    }

    private List<DbReplicationTbl> buildDbReplications(List<String> dbList, Map<String, Long> dbMap, Map<Long, Long> srcMhaDbMappingMap, Map<Long, Long> dstMhaDbMappingMap, String srcTableName) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        for (String dbName : dbList) {
            long dbId = dbMap.get(dbName);
            long srcMhaDbMappingId = srcMhaDbMappingMap.get(dbId);
            long dstMhaDbMappingId = dstMhaDbMappingMap.get(dbId);
            dbReplicationTbls.add(buildDbReplicationTbl(srcTableName, StringUtils.EMPTY, srcMhaDbMappingId, dstMhaDbMappingId, ReplicationTypeEnum.DB_TO_DB.getType()));
        }
        return dbReplicationTbls;
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
}
