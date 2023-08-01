package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.DrcConfigView;
import com.ctrip.framework.drc.console.vo.v2.DrcMhaConfigView;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class DrcBuildServiceV2Impl implements DrcBuildServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;

    private static final String CLUSTER_NAME_SUFFIX = "_dalcluster";
    private static final String DEFAULT_TABLE_NAME = ".*";

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMha(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);
        String clusterName = param.getDstMhaName() + CLUSTER_NAME_SUFFIX;
        MhaTblV2 srcMha = buildMhaTbl(param.getSrcMhaName(), param.getSrcDcId(), param.getBuId(), clusterName);
        MhaTblV2 dstMha = buildMhaTbl(param.getDstMhaName(), param.getDstDcId(), param.getBuId(), clusterName);

        long srcMhaId = insertMha(srcMha);
        long dstMhaId = insertMha(dstMha);
        insertMhaReplication(srcMhaId, dstMhaId);
        insertMhaReplication(dstMhaId, srcMhaId);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildDrc(DrcBuildParam param) throws Exception {
        checkDrcBuildParam(param);
        DrcBuildBaseParam srcBuildParam = param.getSrcBuildParam();
        DrcBuildBaseParam dstBuildParam = param.getDstBuildParam();
        String srcMhaName = srcBuildParam.getMhaName();
        String dstMhaName = dstBuildParam.getMhaName();

        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        MhaReplicationTbl srcMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId(), BooleanEnum.FALSE.getCode());
        MhaReplicationTbl dstMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMha.getId(), srcMha.getId(), BooleanEnum.FALSE.getCode());
        if (srcMhaReplication == null || dstMhaReplication == null) {
            throw ConsoleExceptionUtils.message(String.format("mhaReplication between %s and %s not exist", srcMhaName, dstMhaName));
        }

        configureReplicatorGroup(srcMha.getId(), srcBuildParam.getReplicatorInitGtid(), srcBuildParam.getReplicatorIps());
        configureReplicatorGroup(dstMha.getId(), dstBuildParam.getReplicatorInitGtid(), dstBuildParam.getReplicatorIps());

        configureApplierGroup(srcMhaReplication.getId(), srcBuildParam.getApplierInitGtid(), srcBuildParam.getApplierIps());
        configureApplierGroup(dstMhaReplication.getId(), dstBuildParam.getApplierInitGtid(), dstBuildParam.getApplierIps());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception {
        checkDbReplicationBuildParam(param);
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        String nameFilter = param.getDbName() + "\\." + param.getTableName();
        List<String> dbList = mhaDbMappingService.buildMhaDbMappings(srcMha, dstMha, nameFilter);

        List<DbReplicationTbl> dbReplicationTbls = insertDbReplications(srcMha.getId(), dstMha.getId(), dbList, nameFilter);
        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        return dbReplicationIds;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDbReplications(List<Long> dbReplicationIds) throws Exception {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            throw ConsoleExceptionUtils.message("delete dbReplications are empty!");
        }
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(dbReplicationTbls)) {
            logger.info("dbReplicationTbls not exist");
            return;
        }
        dbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        dbReplicationTblDao.batchUpdate(dbReplicationTbls);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            logger.info("dbReplicationFilterMappingTbls not exist");
            return;
        }
        dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        dbReplicationFilterMappingTblDao.batchUpdate(dbReplicationFilterMappingTbls);
    }

    @Override
    public void buildColumnsFilter(ColumnsFilterCreateParam param) throws Exception {
        checkColumnsFilterCreateParam(param);
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(param.getDbReplicationIds());
        if (CollectionUtils.isEmpty(dbReplicationTbls) || dbReplicationTbls.size() != param.getDbReplicationIds().size()) {
            throw ConsoleExceptionUtils.message("dbReplications not exist!");
        }

        long columnsFilterId = insertColumnsFilter(param.getMode(), param.getColumns());
        insertDbReplicationFilterMappings(param.getDbReplicationIds(), columnsFilterId, FilterTypeEnum.COLUMNS_FILTER.getCode());
    }

    @Override
    public void deleteColumnsFilter(List<Long> dbReplicationIds) throws Exception {
        List<DbReplicationFilterMappingTbl> existFilterMappings = getDbReplicationFilterMappings(dbReplicationIds);

        existFilterMappings.forEach(e -> {
            if (e.getRowsFilterId() != -1L) {
                e.setColumnsFilterId(-1L);
            } else {
                e.setDeleted(BooleanEnum.TRUE.getCode());
            }
        });
        dbReplicationFilterMappingTblDao.batchUpdate(existFilterMappings);

    }

    public void buildRowsFilter(RowsFilterCreateParam param) throws Exception {
        checkRowsFilterCreateParam(param);
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(param.getDbReplicationIds());
        if (CollectionUtils.isEmpty(dbReplicationTbls) || dbReplicationTbls.size() != param.getDbReplicationIds().size()) {
            throw ConsoleExceptionUtils.message("dbReplications not exist!");
        }
        long rowsFilterId = insertRowsFilter(param);
        insertDbReplicationFilterMappings(param.getDbReplicationIds(), rowsFilterId, FilterTypeEnum.ROWS_FILTER.getCode());
    }

    @Override
    public void deleteRowsFilter(List<Long> dbReplicationIds) throws Exception {
        List<DbReplicationFilterMappingTbl> existFilterMappings = getDbReplicationFilterMappings(dbReplicationIds);

        existFilterMappings.forEach(e -> {
            if (e.getColumnsFilterId() != -1L) {
                e.setRowsFilterId(-1L);
            } else {
                e.setDeleted(BooleanEnum.TRUE.getCode());
            }
        });
        dbReplicationFilterMappingTblDao.batchUpdate(existFilterMappings);
    }

    @Override
    public DrcConfigView getDrcConfigView(String srcMhaName, String dstMhaName) throws Exception {
        DrcConfigView drcConfigView = new DrcConfigView();
        DrcMhaConfigView srcMhaConfigView = new DrcMhaConfigView();
        DrcMhaConfigView dstMhaConfigView = new DrcMhaConfigView();
        drcConfigView.setSrcMhaConfigView(srcMhaConfigView);
        drcConfigView.setDstMhaConfigView(dstMhaConfigView);

        srcMhaConfigView.setMhaName(srcMhaName);
        dstMhaConfigView.setMhaName(dstMhaName);

        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());

        buildDrcMhaConfigView(srcMhaConfigView, srcMha, dstMha);
        buildDrcMhaConfigView(dstMhaConfigView, dstMha, srcMha);
        return drcConfigView;
    }

    private void buildDrcMhaConfigView(DrcMhaConfigView mhaConfigView, MhaTblV2 srcMha, MhaTblV2 dstMha) throws Exception {
        if (srcMha == null) {
            return;
        }
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(srcMha.getId(), BooleanEnum.FALSE.getCode());
        if (replicatorGroupTbl != null) {
            List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(replicatorTbls)) {
                List<Long> resourceIds = replicatorTbls.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
                List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
                List<String> replicatorIps = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
                mhaConfigView.setReplicatorIps(replicatorIps);
            }
        }

        if (dstMha == null) {
            return;
        }

        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId());
        if (mhaReplicationTbl != null) {
            ApplierGroupTblV2 applierGroupTbl = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
            if (applierGroupTbl == null) {
                return;
            }
            mhaConfigView.setApplierInitGtid(applierGroupTbl.getGtidInit());
            List<ApplierTblV2> applierTbls = applierTblDao.queryByApplierGroupId(applierGroupTbl.getId(), BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(applierTbls)) {
                List<Long> resourceIds = applierTbls.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
                List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
                List<String> applierIps = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
                mhaConfigView.setApplierIps(applierIps);
            }
        }
    }

    private List<DbReplicationFilterMappingTbl> getDbReplicationFilterMappings(List<Long> dbReplicationIds) throws Exception {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            throw ConsoleExceptionUtils.message("dbReplicationIds are empty!");
        }
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(dbReplicationTbls) || dbReplicationTbls.size() != dbReplicationIds.size()) {
            throw ConsoleExceptionUtils.message("dbReplications not exist!");
        }

        List<DbReplicationFilterMappingTbl> existFilterMappings = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(existFilterMappings)) {
            logger.error("dbReplicationFilterMapping not exist, dbReplicationIds: {}", dbReplicationIds);
            throw ConsoleExceptionUtils.message("dbReplicationFilterMapping not exist!");
        }
        return existFilterMappings;
    }

    private long insertRowsFilter(RowsFilterCreateParam param) throws Exception {
        RowsFilterTblV2 rowsFilter = param.extractRowsFilterTbl();
        List<RowsFilterTblV2> existRowsFilters = rowsFilterTblV2Dao.queryByMode(param.getMode());
        for (RowsFilterTblV2 existRowsFilter : existRowsFilters) {
            if (rowsFilter.getConfigs().equals(existRowsFilter.getConfigs())) {
                return existRowsFilter.getId();
            }
        }

        long rowsFilterId = rowsFilterTblV2Dao.insertReturnId(rowsFilter);
        return rowsFilterId;
    }


    private void insertDbReplicationFilterMappings(List<Long> dbReplicationIds, long filterId, int filterType) throws Exception {
        List<DbReplicationFilterMappingTbl> existFilterMappings = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        Map<Long, DbReplicationFilterMappingTbl> existFilterMappingMap = existFilterMappings.stream().collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, Function.identity()));

        List<DbReplicationFilterMappingTbl> insertFilterMappings = new ArrayList<>();
        List<DbReplicationFilterMappingTbl> updateFilterMappings = new ArrayList<>();
        for (long dbReplicationId : dbReplicationIds) {
            DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl;
            if (existFilterMappingMap.containsKey(dbReplicationId)) {
                dbReplicationFilterMappingTbl = existFilterMappingMap.get(dbReplicationId);
                updateFilterMappings.add(dbReplicationFilterMappingTbl);
            } else {
                dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
                dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationId);
                dbReplicationFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
                insertFilterMappings.add(dbReplicationFilterMappingTbl);
            }

            switch (FilterTypeEnum.getByCode(filterType)) {
                case ROWS_FILTER:
                    dbReplicationFilterMappingTbl.setRowsFilterId(filterId);
                    break;
                case COLUMNS_FILTER:
                    dbReplicationFilterMappingTbl.setColumnsFilterId(filterId);
                    break;
                default:
                    break;
            }
        }

        if (!CollectionUtils.isEmpty(insertFilterMappings)) {
            logger.info("insertDbReplicationFilterMappings insertFilterMappings: {}", insertFilterMappings);
            dbReplicationFilterMappingTblDao.batchInsert(insertFilterMappings);
        }
        if (!CollectionUtils.isEmpty(updateFilterMappings)) {
            logger.info("insertDbReplicationFilterMappings updateFilterMappings: {}", updateFilterMappings);
            dbReplicationFilterMappingTblDao.batchUpdate(updateFilterMappings);
        }
    }

    private long insertColumnsFilter(int mode, List<String> columns) throws Exception {
        String columnsJson = JsonUtils.toJson(columns);
        List<ColumnsFilterTblV2> existColumnsFilters = columnFilterTblV2Dao.queryByMode(mode);
        for (ColumnsFilterTblV2 columnsFilter : existColumnsFilters) {
            if (columnsFilter.getColumns().equals(columnsJson)) {
                return columnsFilter.getId();
            }
        }

        ColumnsFilterTblV2 columnsFilterTblV2 = new ColumnsFilterTblV2();
        columnsFilterTblV2.setDeleted(BooleanEnum.FALSE.getCode());
        columnsFilterTblV2.setColumns(columnsJson);
        columnsFilterTblV2.setMode(mode);
        long columnsFilterId = columnFilterTblV2Dao.insertReturnId(columnsFilterTblV2);
        return columnsFilterId;
    }

    private List<DbReplicationTbl> insertDbReplications(long srcMhaId, long dstMhaId, List<String> dbList, String nameFilter) throws Exception {
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMhaId);
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMhaId);
        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));
        Map<Long, Long> dstMhaDbMappingMap = dstMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbList);
        Map<String, Long> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        if (StringUtils.isBlank(nameFilter)) {
            dbReplicationTbls.addAll(buildDbReplications(dbList, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, DEFAULT_TABLE_NAME));
        } else {
            String[] dbFullNames = nameFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
            String srcTableName = dbFullNames[1];
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(dbFullNames[0]);
            List<String> dbNames = dbList.stream().filter(db -> aviatorRegexFilter.filter(db)).collect(Collectors.toList());

            dbReplicationTbls.addAll(buildDbReplications(dbNames, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, srcTableName));
        }

        checkDbReplicationExist(dbReplicationTbls, srcMhaDbMappings, dstMhaDbMappings);
        dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        logger.info("insertDbReplications size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);
        return dbReplicationTbls;
    }

    private void checkDbReplicationExist(List<DbReplicationTbl> dbReplicationTbls,
                                         List<MhaDbMappingTbl> srcMhaDbMappings,
                                         List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<DbReplicationTbl> existDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        if (!CollectionUtils.isEmpty(existDbReplications)) {
            return;
        }

        List<Long> existDbReplicationIds = new ArrayList<>();
        for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
            for (DbReplicationTbl existDbReplication : existDbReplications) {
                if (dbReplicationTbl.getSrcMhaDbMappingId().equals(existDbReplication.getSrcMhaDbMappingId())
                        && dbReplicationTbl.getDstMhaDbMappingId().equals(existDbReplication.getDstMhaDbMappingId())
                        && dbReplicationTbl.getSrcLogicTableName().equals(existDbReplication.getSrcLogicTableName())) {
                    existDbReplicationIds.add(dbReplicationTbl.getId());
                }
            }
        }

        if (!CollectionUtils.isEmpty(existDbReplicationIds)) {
            logger.error("dbReplication already exist, existDbReplicationIds: {}", existDbReplicationIds);
            throw ConsoleExceptionUtils.message(String.format("dbReplication already exist, existDbReplicationIds: %s", existDbReplicationIds));
        }
    }

    private List<DbReplicationTbl> getExistDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());
        return existDbReplications;
    }

    private List<DbReplicationTbl> buildDbReplications(List<String> dbList,
                                                       Map<String, Long> dbMap,
                                                       Map<Long, Long> srcMhaDbMappingMap,
                                                       Map<Long, Long> dstMhaDbMappingMap,
                                                       String srcTableName) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        for (String dbName : dbList) {
            long dbId = dbMap.get(dbName);
            long srcMhaDbMappingId = srcMhaDbMappingMap.get(dbId);
            long dstMhaDbMappingId = dstMhaDbMappingMap.get(dbId);
            dbReplicationTbls.add(buildDbReplicationTbl(srcTableName, srcMhaDbMappingId, dstMhaDbMappingId, ReplicationTypeEnum.DB_TO_DB.getType()));
        }
        return dbReplicationTbls;
    }

    private DbReplicationTbl buildDbReplicationTbl(String srcTableName, long srcMhaDbMappingId, long dstMhaDbMappingId, int replicationType) {
        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        dbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        dbReplicationTbl.setSrcLogicTableName(srcTableName);
        dbReplicationTbl.setDstLogicTableName("");
        dbReplicationTbl.setReplicationType(replicationType);
        dbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return dbReplicationTbl;
    }

    private void configureApplierGroup(long mhaReplicationId, String applierInitGtid, List<String> applierIps) throws Exception {
        long applierGroupId = insertOrUpdateApplierGroup(mhaReplicationId, applierInitGtid);
        configureAppliers(applierGroupId, applierIps);
    }

    private void configureAppliers(long applierGroupId, List<String> applierIps) throws Exception {
        List<ApplierTblV2> existAppliers = applierTblDao.queryByApplierGroupId(applierGroupId, BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(existAppliers)) {
            logger.info("delete appliers: {}", existAppliers);
            existAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            applierTblDao.batchUpdate(existAppliers);
        }

        if (CollectionUtils.isEmpty(applierIps)) {
            return;
        }

        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(applierIps);
        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ApplierTblV2> applierTbls = applierIps.stream().map(ip -> {
            ApplierTblV2 applierTbl = new ApplierTblV2();
            applierTbl.setApplierGroupId(applierGroupId);
            applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
            applierTbl.setMaster(BooleanEnum.FALSE.getCode());
            applierTbl.setResourceId(resourceTblMap.get(ip));
            applierTbl.setDeleted(BooleanEnum.FALSE.getCode());

            return applierTbl;
        }).collect(Collectors.toList());

        logger.info("insert applierIps: {}", applierIps);
        applierTblDao.batchInsert(applierTbls);
    }

    private long insertOrUpdateApplierGroup(long mhaReplicationId, String applierInitGtid) throws Exception {
        long applierGroupId;
        ApplierGroupTblV2 existApplierGroup = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationId);
        if (existApplierGroup == null) {
            logger.info("buildDrc insert applierGroup, mhaReplicationId: {}", mhaReplicationId);
            ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
            applierGroupTbl.setMhaReplicationId(mhaReplicationId);
            applierGroupTbl.setGtidInit(applierInitGtid);
            applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());

            applierGroupId = applierGroupTblDao.insertWithReturnId(applierGroupTbl);
        } else {
            applierGroupId = existApplierGroup.getId();
            if (existApplierGroup.getDeleted() == BooleanEnum.TRUE.getCode()) {
                existApplierGroup.setDeleted(BooleanEnum.FALSE.getCode());
                applierGroupTblDao.update(existApplierGroup);
            }
        }
        return applierGroupId;
    }

    private void configureReplicatorGroup(long mhaId, String replicatorInitGtid, List<String> replicatorIps) throws Exception {
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaId);
        configureReplicators(replicatorGroupId, replicatorInitGtid, replicatorIps);
    }

    private void configureReplicators(long replicatorGroupId, String replicatorInitGtid, List<String> replicatorIps) throws Exception {
        List<ReplicatorTbl> existReplicators = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupId), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(existReplicators)) {
            logger.info("delete replicators: {}", existReplicators);
            existReplicators.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            replicatorTblDao.batchUpdate(existReplicators);
        }

        if (CollectionUtils.isEmpty(replicatorIps)) {
            return;
        }
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(replicatorIps);
        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ReplicatorTbl> replicatorTbls = new ArrayList<>();
        for (String ip : replicatorIps) {
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            replicatorTbl.setRelicatorGroupId(replicatorGroupId);
            replicatorTbl.setGtidInit(replicatorInitGtid);
            replicatorTbl.setResourceId(resourceTblMap.get(ip));
            replicatorTbl.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
            replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(ip));
            replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
            replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());

            replicatorTbls.add(replicatorTbl);
        }

        logger.info("insert replicatorIps: {}", replicatorIps);
        replicatorTblDao.batchInsert(replicatorTbls);
    }

    private long insertOrUpdateReplicatorGroup(long mhaId) throws Exception {
        long replicatorGroupId;
        ReplicatorGroupTbl existReplicatorGroup = replicatorGroupTblDao.queryByMhaId(mhaId);
        if (existReplicatorGroup == null) {
            logger.info("buildDrc insert replicatorGroup, mhaId: {}", mhaId);
            ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
            replicatorGroupTbl.setMhaId(mhaId);
            replicatorGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return replicatorGroupTblDao.insertWithReturnId(replicatorGroupTbl);
        } else {
            replicatorGroupId = existReplicatorGroup.getId();
            if (existReplicatorGroup.getDeleted() == BooleanEnum.TRUE.getCode()) {
                existReplicatorGroup.setDeleted(BooleanEnum.FALSE.getCode());
                replicatorGroupTblDao.update(existReplicatorGroup);
            }
        }
        return replicatorGroupId;
    }

    private void insertMhaReplication(long srcMhaId, long dstMhaId) throws Exception {
        MhaReplicationTbl existMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId, BooleanEnum.FALSE.getCode());
        if (existMhaReplication != null) {
            logger.info("mhaReplication already exist, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            return;
        }

        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setSrcMhaId(srcMhaId);
        mhaReplicationTbl.setDstMhaId(dstMhaId);
        mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        mhaReplicationTbl.setDrcStatus(BooleanEnum.FALSE.getCode());

        logger.info("insertMhaReplication srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        mhaReplicationTblDao.insert(mhaReplicationTbl);
    }

    private long insertMha(MhaTblV2 mhaTblV2) throws Exception {
        MhaTblV2 existMhaTbl = mhaTblDao.queryByMhaName(mhaTblV2.getMhaName());
        if (null == existMhaTbl) {
            return mhaTblDao.insertWithReturnId(mhaTblV2);
        } else if (existMhaTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
            existMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaTblDao.update(existMhaTbl);
        }
        return existMhaTbl.getId();
    }

    private MhaTblV2 buildMhaTbl(String mhaName, long dcId, long buId, String clusterName) {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName(mhaName);
        mhaTblV2.setDcId(dcId);
        mhaTblV2.setApplyMode(ApplyMode.transaction_table.getType());
        mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
        mhaTblV2.setBuId(buId);
        mhaTblV2.setClusterName(clusterName);
        mhaTblV2.setAppId(-1L);
        mhaTblV2.setDeleted(BooleanEnum.FALSE.getCode());

        mhaTblV2.setReadUser(monitorTableSourceProvider.getReadUserVal());
        mhaTblV2.setReadPassword(monitorTableSourceProvider.getReadPasswordVal());
        mhaTblV2.setWriteUser(monitorTableSourceProvider.getWriteUserVal());
        mhaTblV2.setWritePassword(monitorTableSourceProvider.getWritePasswordVal());
        mhaTblV2.setMonitorUser(monitorTableSourceProvider.getMonitorUserVal());
        mhaTblV2.setMonitorPassword(monitorTableSourceProvider.getMonitorPasswordVal());

        return mhaTblV2;
    }

    private void checkDrcBuildParam(DrcBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcBuildParam().getMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstBuildParam().getMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkArgument(!param.getSrcBuildParam().getMhaName().equals(param.getDstBuildParam().getMhaName()), "srcMha and dstMha are same!");
    }

    private void checkDrcMhaBuildParam(DrcMhaBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkId(param.getBuId(), "buId requires not null!");
        PreconditionUtils.checkId(param.getSrcDcId(), "srcDcId requires not null!");
        PreconditionUtils.checkId(param.getDstDcId(), "dstDcId requires not null!");
    }

    private void checkDbReplicationBuildParam(DbReplicationBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDbName(), "dbName requires not empty!");
        PreconditionUtils.checkString(param.getTableName(), "tableName requires not empty!");
    }

    private void checkColumnsFilterCreateParam(ColumnsFilterCreateParam param) {
        PreconditionUtils.checkArgument(!CollectionUtils.isEmpty(param.getDbReplicationIds()), "dbReplicationIds require not empty!");
        PreconditionUtils.checkArgument(!CollectionUtils.isEmpty(param.getColumns()), "columns require not empty!");
        PreconditionUtils.checkArgument(ColumnsFilterModeEnum.checkMode(param.getMode()), "columnsFilter mode not support!");
    }

    private void checkRowsFilterCreateParam(RowsFilterCreateParam param) {
        PreconditionUtils.checkArgument(!CollectionUtils.isEmpty(param.getDbReplicationIds()), "dbReplicationIds require not empty!");
        PreconditionUtils.checkArgument(RowsFilterModeEnum.checkMode(param.getMode()), "rowsFilter mode not support!");
    }
}
