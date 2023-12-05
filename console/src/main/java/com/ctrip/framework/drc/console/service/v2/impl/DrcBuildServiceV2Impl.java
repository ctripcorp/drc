package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.service.log.LogBlackListType;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.*;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.ADD_REMOVE_PAIR_SIZE;
import static com.ctrip.framework.drc.console.config.ConsoleConfig.DEFAULT_APPLIER_PORT;

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
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
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
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private CacheMetaService cacheMetaService;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DbMetaCorrectService dbMetaCorrectService;
    @Autowired
    private ConflictLogService conflictLogService;


    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "drcMetaRefreshV2");
    private static final String CLUSTER_NAME_SUFFIX = "_dalcluster";
    private static final String DEFAULT_TABLE_NAME = ".*";

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMha(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);

        long buId = getBuId(param.getBuName());
        DcTbl srcDcTbl = dcTblDao.queryByDcName(param.getSrcDc());
        DcTbl dstDcTbl = dcTblDao.queryByDcName(param.getDstDc());
        if (srcDcTbl == null || dstDcTbl == null) {
            throw ConsoleExceptionUtils.message(String.format("srcDc: %s or dstDc: %s not exist", param.getSrcDc(), param.getDstDc()));
        }

        MhaTblV2 srcMha = buildMhaTbl(param.getSrcMhaName(), srcDcTbl.getId(), buId, param.getSrcTag());
        MhaTblV2 dstMha = buildMhaTbl(param.getDstMhaName(), dstDcTbl.getId(), buId, param.getDstTag());

        long srcMhaId = insertMha(srcMha);
        long dstMhaId = insertMha(dstMha);
        insertMhaReplication(srcMhaId, dstMhaId);
        insertMhaReplication(dstMhaId, srcMhaId);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMessengerMha(MessengerMhaBuildParam param) throws Exception {
        checkMessengerMhaBuildParam(param);

        long buId = getBuId(param.getBuName().trim());
        DcTbl dcTbl = dcTblDao.queryByDcName(param.getDc().trim());
        if (dcTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "dc not exist: " + param.getDc());
        }

        MhaTblV2 mhaTbl = buildMhaTbl(param.getMhaName().trim(), dcTbl.getId(), buId, param.getTag());
        long mhaId = insertMha(mhaTbl);

        // messengerGroup
        Long srcReplicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(mhaId);
        messengerGroupTblDao.upsertIfNotExist(mhaId, srcReplicatorGroupId, "");
    }

    @Override
    public String buildDrc(DrcBuildParam param) throws Exception {
        submitDrc(param);

        Drc drc = metaInfoService.getDrcReplicationConfig(param.getSrcBuildParam().getMhaName(), param.getDstBuildParam().getMhaName());
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProvider scheduledTask error, {}", e);
        }
        return XmlUtils.formatXML(drc.toString());
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void submitDrc(DrcBuildParam param) throws Exception {
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
        if (srcMhaReplication == null) {
            insertMhaReplication(srcMha.getId(), dstMha.getId());
            srcMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId(), BooleanEnum.FALSE.getCode());
        }
        if (dstMhaReplication == null) {
            insertMhaReplication(dstMha.getId(), srcMha.getId());
            dstMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMha.getId(), srcMha.getId(), BooleanEnum.FALSE.getCode());
        }
        
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        configureReplicatorGroup(srcMha, srcBuildParam.getReplicatorInitGtid(), srcBuildParam.getReplicatorIps(), resourceTbls);
        configureReplicatorGroup(dstMha, dstBuildParam.getReplicatorInitGtid(), dstBuildParam.getReplicatorIps(), resourceTbls);

        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());
        List<DbReplicationTbl> srcDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        List<DbReplicationTbl> dstDbReplications = getExistDbReplications(dstMhaDbMappings, srcMhaDbMappings);

        configureApplierGroup(srcMhaReplication.getId(), dstBuildParam.getApplierInitGtid(), dstBuildParam.getApplierIps(), resourceTbls, srcDbReplications);
        configureApplierGroup(dstMhaReplication.getId(), srcBuildParam.getApplierInitGtid(), srcBuildParam.getApplierIps(), resourceTbls, dstDbReplications);

        if (!CollectionUtils.isEmpty(srcBuildParam.getApplierIps())) {
            dstMhaReplication.setDrcStatus(BooleanEnum.TRUE.getCode());
            mhaReplicationTblDao.update(dstMhaReplication);
        } else {
            dstMhaReplication.setDrcStatus(BooleanEnum.FALSE.getCode());
            mhaReplicationTblDao.update(dstMhaReplication);
        }
        if (!CollectionUtils.isEmpty(dstBuildParam.getApplierIps())) {
            srcMhaReplication.setDrcStatus(BooleanEnum.TRUE.getCode());
            mhaReplicationTblDao.update(srcMhaReplication);
        } else {
            srcMhaReplication.setDrcStatus(BooleanEnum.FALSE.getCode());
            mhaReplicationTblDao.update(srcMhaReplication);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildDbReplicationConfig(DbReplicationBuildParam param) throws Exception {
        List<Long> dbReplicationIds = configureDbReplications(param);

        RowsFilterCreateParam rowsFilterCreateParam = param.getRowsFilterCreateParam();
        ColumnsFilterCreateParam columnsFilterCreateParam = param.getColumnsFilterCreateParam();
        if (rowsFilterCreateParam != null) {
            rowsFilterCreateParam.setDbReplicationIds(dbReplicationIds);
            buildRowsFilter(rowsFilterCreateParam);
        } else {
            deleteRowsFilter(dbReplicationIds);
        }

        if (columnsFilterCreateParam != null) {
            columnsFilterCreateParam.setDbReplicationIds(dbReplicationIds);
            buildColumnsFilter(columnsFilterCreateParam);
        } else {
            deleteColumnsFilter(dbReplicationIds);
        }
    }

    @Override
    public Pair<Long, Long> checkDbReplicationFilter(List<Long> dbReplicationIds) throws Exception {
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            return Pair.of(-1L, -1L);
        }

        if (dbReplicationFilterMappingTbls.size() != dbReplicationIds.size()) {
            throw ConsoleExceptionUtils.message("dbReplication contains different filterMappings");
        }

        List<Long> rowsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getRowsFilterId).distinct().collect(Collectors.toList());
        List<Long> columnsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).distinct().collect(Collectors.toList());
        if (rowsFilterIds.size() != 1) {
            throw ConsoleExceptionUtils.message("dbReplication contains different rowsFilter");
        }
        if (columnsFilterIds.size() != 1) {
            throw ConsoleExceptionUtils.message("dbReplication contains different columnsFilter");
        }
        return Pair.of(rowsFilterIds.get(0), columnsFilterIds.get(0));
    }

    @Override
    public List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception {
        logger.info("configureDbReplications param: {}", param);
        checkDbReplicationBuildParam(param);

        //update
        if (!CollectionUtils.isEmpty(param.getDbReplicationIds())) {
            return updateDbReplications(param);
        }

        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        String nameFilter = param.getDbName() + "\\." + param.getTableName();
        if (consoleConfig.getCflBlackListAutoAddSwitch()) {
            addConflictBlackList(nameFilter); // async , fail not block configureDbReplications
        }

        Pair<List<String>, List<String>> dbTablePair = mhaDbMappingService.initMhaDbMappings(srcMha, dstMha, nameFilter);
        List<DbReplicationTbl> dbReplicationTbls = insertDbReplications(srcMha, dstMha.getId(), dbTablePair, nameFilter);
        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        return dbReplicationIds;
    }

    private void addConflictBlackList(String nameFilter) {
        executorService.submit(() -> {
            try {
                conflictLogService.addDbBlacklist(nameFilter, LogBlackListType.AUTO);
            } catch (Exception e) {
                logger.error("addDbBlacklist error", e);
            }
        });
    }

    public List<Long> updateDbReplications(DbReplicationBuildParam param) throws Exception {
        List<Long> dbReplicationIds = param.getDbReplicationIds();
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(dbReplicationIds);
        if (dbReplicationTbls.size() != dbReplicationIds.size()) {
            throw ConsoleExceptionUtils.message("dbReplications not exist!");
        }

        List<String> srcLogicTableNames = dbReplicationTbls.stream().map(DbReplicationTbl::getSrcLogicTableName).distinct().collect(Collectors.toList());
        if (srcLogicTableNames.size() > 1) {
            throw ConsoleExceptionUtils.message("dbReplications contains different tables");
        }

        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());

        String nameFilter = param.getDbName() + "\\." + param.getTableName();
        List<String> tableList = mysqlServiceV2.queryTablesWithNameFilter(srcMha.getMhaName(), nameFilter);
        checkExistDbReplication(tableList, dbReplicationIds, srcMhaDbMappings, dstMhaDbMappings);

        if (!param.getTableName().equals(srcLogicTableNames.get(0))) {
            dbReplicationTbls.forEach(e -> e.setSrcLogicTableName(param.getTableName()));
            dbReplicationTblDao.update(dbReplicationTbls);
            logger.info("update dbReplicationTbls: {}", dbReplicationTbls);
        }

        Pair<Long, Long> filterPair = checkDbReplicationFilter(dbReplicationIds);
        long rowsFilterId = filterPair.getLeft();
        long columnsFilterId = filterPair.getRight();

        Set<String> filterColumns = new HashSet<>();
        if (rowsFilterId != -1L) {
            RowsFilterTblV2 rowsFilterTblV2 = rowsFilterTblV2Dao.queryById(rowsFilterId);
            RowsFilterConfigView rowsConfigView = buildRowsFilterConfigView(rowsFilterTblV2);
            if (!CollectionUtils.isEmpty(rowsConfigView.getColumns())) {
                filterColumns.addAll(rowsConfigView.getColumns());
            }
            if (!CollectionUtils.isEmpty(rowsConfigView.getUdlColumns())) {
                filterColumns.addAll(rowsConfigView.getUdlColumns());
            }
        }

        if (columnsFilterId != -1L) {
            ColumnsFilterTblV2 columnsFilterTblV2 = columnFilterTblV2Dao.queryById(columnsFilterId);
            ColumnsConfigView columnsConfigView = buildColumnsConfigView(columnsFilterTblV2);
            filterColumns.addAll(columnsConfigView.getColumns());
        }

        Set<String> commonColumns = mysqlServiceV2.getCommonColumnIn(param.getSrcMhaName(), param.getDbName(), param.getTableName());
        if (!commonColumns.containsAll(filterColumns)) {
            throw ConsoleExceptionUtils.message("rowsFilter or columnsFilter columns not match");
        }
        return dbReplicationIds;
    }

    @Override
    public List<DbReplicationView> getDbReplicationView(String srcMhaName, String dstMhaName) throws Exception {
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }

        List<DbReplicationView> views = new ArrayList<>();
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());
        List<DbReplicationTbl> existDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        if (CollectionUtils.isEmpty(existDbReplications)) {
            return views;
        }

        List<Long> dbReplicationIds = existDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        List<DbReplicationFilterMappingTbl> filterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        Map<Long, DbReplicationFilterMappingTbl> filterMappingTblsMap = filterMappingTbls.stream().collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, Function.identity()));

        List<Long> srcDbIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(srcDbIds);
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

        views = existDbReplications.stream().map(source -> {
            DbReplicationView target = new DbReplicationView();
            target.setDbReplicationId(source.getId());
            target.setLogicTableName(source.getSrcLogicTableName());
            long dbId = srcMhaDbMappingMap.get(source.getSrcMhaDbMappingId());
            target.setDbName(dbTblMap.get(dbId));
            target.setCreateTime(DateUtils.longToString(source.getCreateTime().getTime()));
            target.setUpdateTime(DateUtils.longToString(source.getDatachangeLasttime().getTime()));

            DbReplicationFilterMappingTbl filterMapping = filterMappingTblsMap.get(source.getId());
            if (filterMapping != null) {
                List<Integer> filterTypes = new ArrayList<>();
                if (filterMapping.getRowsFilterId() != -1L) {
                    filterTypes.add(FilterTypeEnum.ROWS_FILTER.getCode());
                }
                if (filterMapping.getColumnsFilterId() != -1) {
                    filterTypes.add(FilterTypeEnum.COLUMNS_FILTER.getCode());
                }
                target.setFilterTypes(filterTypes);
            }
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDbReplications(List<Long> dbReplicationIds) throws Exception {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            throw ConsoleExceptionUtils.message("dbReplicationIds require not empty");
        }
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByIds(dbReplicationIds);
        if (CollectionUtils.isEmpty(dbReplicationTbls) || dbReplicationTbls.size() != dbReplicationIds.size()) {
            throw ConsoleExceptionUtils.message("dbReplications not exist");
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
    public ColumnsConfigView getColumnsConfigView(long dbReplicationId) throws Exception {
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationId(dbReplicationId);
        if (CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            return null;
        }
        long columnsFilterId = dbReplicationFilterMappingTbls.get(0).getColumnsFilterId();
        if (columnsFilterId == -1L) {
            return null;
        }

        ColumnsFilterTblV2 columnsFilterTblV2 = columnFilterTblV2Dao.queryById(columnsFilterId);
        return buildColumnsConfigView(columnsFilterTblV2);
    }

    @Override
    public void deleteColumnsFilter(List<Long> dbReplicationIds) throws Exception {
        List<DbReplicationFilterMappingTbl> existFilterMappings = getDbReplicationFilterMappings(dbReplicationIds);
        if (CollectionUtils.isEmpty(existFilterMappings)) {
            logger.info("deleteColumnsFilter filterMapping is empty, dbReplicationIds: {}");
            return;
        }

        existFilterMappings.forEach(e -> {
            if (e.getRowsFilterId() != -1L) {
                e.setColumnsFilterId(-1L);
            } else {
                e.setDeleted(BooleanEnum.TRUE.getCode());
            }
        });
        dbReplicationFilterMappingTblDao.batchUpdate(existFilterMappings);

    }

    @Override
    public RowsFilterConfigView getRowsConfigView(long dbReplicationId) throws Exception {
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationId(dbReplicationId);
        if (CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            return null;
        }
        long rowsFilterId = dbReplicationFilterMappingTbls.get(0).getRowsFilterId();
        if (rowsFilterId == -1L) {
            return null;
        }
        RowsFilterTblV2 rowsFilterTblV2 = rowsFilterTblV2Dao.queryById(rowsFilterId);
        return buildRowsFilterConfigView(rowsFilterTblV2);
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
        if (CollectionUtils.isEmpty(existFilterMappings)) {
            logger.info("deleteColumnsFilter filterMapping is empty, dbReplicationIds: {}");
            return;
        }

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
    public List<String> getMhaAppliers(String srcMhaName, String dstMhaName) throws Exception {
        if (StringUtils.isBlank(srcMhaName) || StringUtils.isBlank(dstMhaName)) {
            return new ArrayList<>();
        }
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            return new ArrayList<>();
        }

        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId());
        if (mhaReplicationTbl == null) {
            return new ArrayList<>();
        }
        ApplierGroupTblV2 applierGroupTbl = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
        if (applierGroupTbl == null) {
            return new ArrayList<>();
        }
        List<ApplierTblV2> applierTbls = applierTblDao.queryByApplierGroupId(applierGroupTbl.getId(), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(applierTbls)) {
            return new ArrayList<>();
        }
        List<Long> resourceIds = applierTbls.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        List<String> applierIps = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        return applierIps;
    }

    @Override
    public String getApplierGtid(String srcMhaName, String dstMhaName) throws Exception {
        String applierGtid = "";
        if (StringUtils.isBlank(srcMhaName) || StringUtils.isBlank(dstMhaName)) {
            return applierGtid;
        }
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            return applierGtid;
        }

        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId());
        if (mhaReplicationTbl == null) {
            return applierGtid;
        }
        ApplierGroupTblV2 applierGroupTbl = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
        if (applierGroupTbl == null) {
            return applierGtid;
        }
        return applierGroupTbl.getGtidInit();
    }

    @Override
    public String buildMessengerDrc(MessengerMetaDto dto) throws Exception {
        this.doBuildMessengerDrc(dto);
        Drc drcMessengerConfig = metaInfoService.getDrcMessengerConfig(dto.getMhaName());
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProviderV2.scheduledTask error. req: " + dto, e);
        }
        return XmlUtils.formatXML(drcMessengerConfig.toString());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MhaTblV2 syncMhaInfoFormDbaApi(String mhaName) throws SQLException {
        MhaTblV2 existMha = mhaTblDao.queryByMhaName(mhaName);
        if (existMha != null) {
            throw ConsoleExceptionUtils.message("mhaName already exist!");
        }
        DbaClusterInfoResponse clusterMembersInfo = dbaApiService.getClusterMembersInfo(mhaName);
        List<MemberInfo> memberlist = clusterMembersInfo.getData().getMemberlist();
        String dcInDbaSystem = memberlist.stream().findFirst().map(MemberInfo::getMachine_located_short).get();
        Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
        String dcInDrc = dbaDc2DrcDcMap.getOrDefault(dcInDbaSystem.toLowerCase(), null);
        DcTbl dcTbl = dcTblDao.queryByDcName(dcInDrc);
        MhaTblV2 mhaTblV2 = buildMhaTbl(mhaName, dcTbl.getId(), 1L, ResourceTagEnum.COMMON.getName());
        mhaTblV2.setMonitorSwitch(BooleanEnum.TRUE.getCode());
        Long mhaId = mhaTblDao.insertWithReturnId(mhaTblV2);
        logger.info("[[mha={}]] syncMhaInfoFormDbaApi mhaTbl affect mhaId:{}", mhaName, mhaId);

        List<MachineTbl> machinesToBeInsert = new ArrayList<>();
        for (MemberInfo memberInfo : memberlist) {
            machinesToBeInsert.add(extractFrom(memberInfo, mhaId));
        }
        int[] ints = machineTblDao.batchInsert(machinesToBeInsert);
        logger.info("[[mha={}]] syncMhaInfoFormDbaApi machineTbl affect rows:{}", mhaName, Arrays.stream(ints).sum());

        mhaTblV2.setId(mhaId);
        return mhaTblV2;
    }


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void syncMhaDbInfoFromDbaApiIfNeeded(MhaTblV2 existMha, List<MachineDto> machineDtos) throws Exception {
        Long mhaId = existMha.getId();
        String mhaName = existMha.getMhaName();
        List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        boolean synced = machineTbls.stream().anyMatch(e -> e.getMaster().equals(BooleanEnum.TRUE.getCode()));
        if (synced) {
            logger.info("{} is already synced", mhaName);
            return;
        }

        List<MachineTbl> machineFromDba = new ArrayList<>();
        for (MachineDto memberInfo : machineDtos) {
            MachineTbl machineTbl = extractFrom(memberInfo, mhaId);
            machineFromDba.add(machineTbl);
        }
        dbMetaCorrectService.mhaInstancesChange(machineFromDba, existMha);
    }

    @Override
    public void autoConfigReplicatorsWithRealTimeGtid(MhaTblV2 mhaTbl) throws SQLException {
        String mhaExecutedGtid = mysqlServiceV2.getMhaExecutedGtid(mhaTbl.getMhaName());
        autoConfigReplicatorsWithGtid(mhaTbl, mhaExecutedGtid);
    }

    @Override
    public void autoConfigReplicatorsWithGtid(MhaTblV2 mhaTbl, String gtidInit) throws SQLException {
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTbl.getId());
        Long rGroupId = replicatorGroupTbl.getId();
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(rGroupId), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(replicatorTbls)) {
            logger.warn("[[mha={}]] replicator exist,do nothing", mhaTbl.getMhaName());
        } else {
            ResourceSelectParam selectParam = new ResourceSelectParam();
            selectParam.setType(ModuleEnum.REPLICATOR.getCode());
            selectParam.setMhaName(mhaTbl.getMhaName());
            List<ResourceView> resourceViews = resourceService.autoConfigureResource(selectParam);
            if (CollectionUtils.isEmpty(resourceViews)) {
                logger.error("[[mha={}]] autoConfigureResource failed", mhaTbl.getMhaName());
                throw new ConsoleException("autoConfigReplicators failed!");
            } else {
                List<ReplicatorTbl> insertReplicators = Lists.newArrayList();
                for (ResourceView resourceView : resourceViews) {
                    ReplicatorTbl replicatorTbl = new ReplicatorTbl();
                    replicatorTbl.setRelicatorGroupId(rGroupId);
                    replicatorTbl.setGtidInit(gtidInit);
                    replicatorTbl.setResourceId(resourceView.getResourceId());
                    replicatorTbl.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
                    replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(resourceView.getIp()));
                    replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
                    replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());
                    insertReplicators.add(replicatorTbl);
                }
                int[] ints = replicatorTblDao.batchInsert(insertReplicators);
                logger.info("[[mha={}]] autoConfigReplicatorsWithRealTimeGtid ,excepted:{},actual:{}",
                        mhaTbl.getMhaName(), insertReplicators.size(), Arrays.stream(ints).sum());
            }
        }
    }


    @Override
    public void autoConfigAppliers(MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, String gtid) throws SQLException {
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), destMhaTbl.getId(), BooleanEnum.FALSE.getCode());
        if (mhaReplicationTbl == null) {
            throw ConsoleExceptionUtils.message(String.format("configure appliers fail, mha replication not init yet! drc: %s->%s", srcMhaTbl.getMhaName(), destMhaTbl.getMhaName()));
        }
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
        if (applierGroupTblV2 == null) {
            throw ConsoleExceptionUtils.message(String.format("configure appliers fail, applier group not init yet! drc: %s->%s", srcMhaTbl.getMhaName(), destMhaTbl.getMhaName()));
        }
        autoConfigAppliers(mhaReplicationTbl, applierGroupTblV2, srcMhaTbl, destMhaTbl, gtid);
    }

    @Override
    public void autoConfigAppliersWithRealTimeGtid(
            MhaReplicationTbl mhaReplicationTbl, ApplierGroupTblV2 applierGroup,
            MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl) throws SQLException {
        String mhaExecutedGtid = mysqlServiceV2.getMhaExecutedGtid(srcMhaTbl.getMhaName());
        if (StringUtils.isBlank(mhaExecutedGtid)) {
            logger.error("[[mha={}]] getMhaExecutedGtid failed", srcMhaTbl.getMhaName());
            throw new ConsoleException("getMhaExecutedGtid failed!");
        }
        autoConfigAppliers(mhaReplicationTbl, applierGroup, srcMhaTbl, destMhaTbl, mhaExecutedGtid);
    }

    private void autoConfigAppliers(MhaReplicationTbl mhaReplicationTbl, ApplierGroupTblV2 applierGroup, MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, String mhaExecutedGtid) throws SQLException {
        // applier group gtid
        if (!StringUtils.isBlank(mhaExecutedGtid)) {
            applierGroup.setGtidInit(mhaExecutedGtid);
            applierGroupTblDao.update(applierGroup);
            logger.info("[[mha={}]] autoConfigAppliers with gtid:{}", destMhaTbl.getMhaName(), mhaExecutedGtid);
        }
        // mha replication
        mhaReplicationTbl.setDrcStatus(1);
        mhaReplicationTblDao.update(mhaReplicationTbl);
        logger.info("[[mha={}]] autoConfigAppliers update mhaReplicationTbl drcStatus to 1", destMhaTbl.getMhaName());

        // appliers
        List<ApplierTblV2> existAppliers = applierTblDao.queryByApplierGroupId(applierGroup.getId(), BooleanEnum.FALSE.getCode());
        List<Long> inUseResourceId = existAppliers.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
        List<String> inUseIps = resourceTblDao.queryByIds(inUseResourceId).stream().map(ResourceTbl::getIp).collect(Collectors.toList());

        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.APPLIER.getCode());
        selectParam.setMhaName(destMhaTbl.getMhaName());
        selectParam.setSelectedIps(inUseIps);
        List<ResourceView> resourceViews = resourceService.handOffResource(selectParam);
        if (CollectionUtils.isEmpty(resourceViews)) {
            logger.error("[[mha={}]] autoConfigAppliers failed", destMhaTbl.getMhaName());
            throw new ConsoleException("autoConfigAppliers failed!");
        }

        // insert new appliers
        List<ApplierTblV2> insertAppliers = resourceViews.stream()
                .filter(e -> !inUseResourceId.contains(e.getResourceId()))
                .map(e -> buildApplierTbl(applierGroup, e))
                .collect(Collectors.toList());

        // delete old appliers
        List<Long> newResourceId = resourceViews.stream().map(ResourceView::getResourceId).collect(Collectors.toList());
        List<ApplierTblV2> deleteAppliers = existAppliers.stream()
                .filter(e -> !newResourceId.contains(e.getResourceId()))
                .collect(Collectors.toList());
        deleteAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));

        applierTblDao.batchInsert(insertAppliers);
        applierTblDao.batchUpdate(deleteAppliers);
        logger.info("[[mha={}]] autoConfigAppliers success", destMhaTbl.getMhaName());
    }

    private static ApplierTblV2 buildApplierTbl(ApplierGroupTblV2 applierGroup, ResourceView resourceView) {
        ApplierTblV2 applierTbl = new ApplierTblV2();
        applierTbl.setApplierGroupId(applierGroup.getId());
        applierTbl.setResourceId(resourceView.getResourceId());
        applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
        applierTbl.setMaster(BooleanEnum.FALSE.getCode());
        applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return applierTbl;
    }

    @Override
    public void autoConfigMessengersWithRealTimeGtid(MhaTblV2 mhaTbl) throws SQLException {
        String mhaExecutedGtid = mysqlServiceV2.getMhaExecutedGtid(mhaTbl.getMhaName());
        if (StringUtils.isBlank(mhaExecutedGtid)) {
            logger.error("[[mha={}]] getMhaExecutedGtid failed", mhaTbl.getMhaName());
            throw new ConsoleException("getMhaExecutedGtid failed!");
        }
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
        messengerGroupTbl.setGtidExecuted(mhaExecutedGtid);
        messengerGroupTblDao.update(messengerGroupTbl);
        logger.info("[[mha={}]] autoConfigMessengersWithRealTimeGtid with gtid:{}", mhaTbl.getMhaName(), mhaExecutedGtid);

        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.APPLIER.getCode());
        selectParam.setMhaName(mhaTbl.getMhaName());
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(selectParam);
        if (CollectionUtils.isEmpty(resourceViews)) {
            logger.error("[[mha={}]] autoConfigMessengers failed", mhaTbl.getMhaName());
            throw new ConsoleException("autoConfigMessengers failed!");
        } else {
            List<MessengerTbl> insertMessengers = Lists.newArrayList();
            for (ResourceView resourceView : resourceViews) {
                MessengerTbl messengerTbl = new MessengerTbl();
                messengerTbl.setMessengerGroupId(messengerGroupTbl.getId());
                messengerTbl.setResourceId(resourceView.getResourceId());
                messengerTbl.setPort(DEFAULT_APPLIER_PORT);
                messengerTbl.setDeleted(BooleanEnum.FALSE.getCode());
                insertMessengers.add(messengerTbl);
            }
            int[] ints = messengerTblDao.batchInsert(insertMessengers);
            logger.info("[[mha={}]] autoConfigMessengers success", mhaTbl.getMhaName());
        }

    }

    @Override
    public String submitProxyRouteConfig(RouteDto routeDto) {
        try {
            Long routeOrgId = StringUtils.isBlank(routeDto.getRouteOrgName()) ? 0L : buTblDao.queryByBuName(routeDto.getRouteOrgName()).getId();
            Long srcDcId = dcTblDao.queryByDcName(routeDto.getSrcDcName()).getId();
            Long dstDcId = dcTblDao.queryByDcName(routeDto.getDstDcName()).getId();
            List<Long> srcProxyIds = Lists.newArrayList();
            List<Long> relayProxyIds = Lists.newArrayList();
            List<Long> dstProxyIds = Lists.newArrayList();
            for (String proxyUri : routeDto.getSrcProxyUris()) {
                srcProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            for (String proxyUri : routeDto.getRelayProxyUris()) {
                relayProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            for (String proxyUri : routeDto.getDstProxyUris()) {
                dstProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            routeTblDao.upsert(routeOrgId, srcDcId, dstDcId, StringUtils.join(srcProxyIds, ","),
                    StringUtils.join(relayProxyIds, ","), StringUtils.join(dstProxyIds, ","), routeDto.getTag(), routeDto.getDeleted());
            return "update proxy route succeeded";
        } catch (SQLException e) {
            logger.error("update proxy route failed, ", e);
            return "update proxy route failed";
        }
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void doBuildMessengerDrc(MessengerMetaDto dto) throws Exception {

        // 0. check
        MhaTblV2 mhaTbl = mhaTblDao.queryByMhaName(dto.getMhaName(), BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not recorded");
        }
        if (!CollectionUtils.isEmpty(dto.getMessengerIps())) {
            List<MqConfigVo> mqConfigVos = messengerServiceV2.queryMhaMessengerConfigs(dto.getMhaName());
            if (CollectionUtils.isEmpty(mqConfigVos)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Add mq config before put messengers!");
            }
        }
        // 1. configure and persistent in database
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaTbl.getId());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        configureReplicators(mhaTbl.getMhaName(), replicatorGroupId, dto.getrGtidExecuted(), dto.getReplicatorIps(), resourceTbls);
        configureMessengers(mhaTbl, replicatorGroupId, dto.getMessengerIps(), dto.getaGtidExecuted());
    }

    public Long configureMessengers(MhaTblV2 mhaTbl,
                                    Long replicatorGroupId,
                                    List<String> messengerIps,
                                    String gtidExecuted) throws SQLException {
        Long messengerGroupId = configureMessengerGroup(mhaTbl, replicatorGroupId, gtidExecuted);
        configureMessengerInstances(mhaTbl, messengerIps, messengerGroupId);
        return messengerGroupId;
    }

    protected Long configureMessengerGroup(MhaTblV2 mhaTbl, Long replicatorGroupId, String gtidExecuted) throws SQLException {
        String mhaName = mhaTbl.getMhaName();
        Long mhaId = mhaTbl.getId();
        logger.info("[[mha={}, mhaId={},replicatorGroupId={}]]configure or update messenger group", mhaName, mhaId, replicatorGroupId);
        return messengerGroupTblDao.upsertIfNotExist(mhaId, replicatorGroupId, formatGtid(gtidExecuted));
    }

    protected void configureMessengerInstances(MhaTblV2 mhaTbl, List<String> messengerIps, Long messengerGroupId) throws SQLException {
        String mhaName = mhaTbl.getMhaName();

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupId);
        List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());

        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        List<String> messengersInuse = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());

        List<List<String>> addRemoveMessengerIpsPair = getRemoveAndAddInstanceIps(messengersInuse, messengerIps);
        if (ADD_REMOVE_PAIR_SIZE != addRemoveMessengerIpsPair.size()) {
            logger.info("[[mha={}]] wrong add remove messenger pair size {}!={}",
                    mhaName, addRemoveMessengerIpsPair.size(), ADD_REMOVE_PAIR_SIZE);
            return;
        }

        List<String> messengerIpsToBeAdded = addRemoveMessengerIpsPair.get(0);
        List<String> messengerIpsToBeRemoved = addRemoveMessengerIpsPair.get(1);
        logger.info("[[mha={}]]try add messenger {}, remove messenger {}", mhaName, messengerIpsToBeAdded, messengerIpsToBeRemoved);

        List<String> messengerInstancesAdded = addMessengerInstances(messengerIpsToBeAdded, mhaTbl, messengerGroupId);
        List<String> messengerInstancesRemoved = removeMessengerInstances(messengerIpsToBeRemoved, mhaName, messengerGroupId, resourceTbls, messengerTbls);
        logger.info("added M:{}, removed M:{}", messengerInstancesAdded, messengerInstancesRemoved);
    }

    protected List<String> addMessengerInstances(List<String> messengerIpsToBeAdded, MhaTblV2 mhaTbl, Long messengerGroupId) {
        logger.info("[[mha={}]]try add messengers {}", mhaTbl.getMhaName(), messengerIpsToBeAdded);
        List<String> messengerInstancesAdded = Lists.newArrayList();
        String mhaName = mhaTbl.getMhaName();
        try {
            List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(messengerIpsToBeAdded);
            Map<String, ResourceTbl> ipMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, e -> e));
            for (String ip : messengerIpsToBeAdded) {
                logger.info("[[mha={}]]add messenger: {}", mhaName, ip);
                ResourceTbl resourceTbl = ipMap.get(ip);
                if (null == resourceTbl) {
                    logger.info("[[mha={}]]UNLIKELY-messenger resource({}) should already be loaded", mhaName, ip);
                    continue;
                }
                logger.info("[[mha={}]]configure messenger instance: {}", mhaName, ip);
                messengerTblDao.insertMessenger(DEFAULT_APPLIER_PORT, resourceTbl.getId(), messengerGroupId);
                messengerInstancesAdded.add(ip);
            }
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DAO_TBL_EXCEPTION, e);
        }
        return messengerInstancesAdded;
    }

    protected List<String> removeMessengerInstances(
            List<String> messengerIpsToBeRemoved,
            String mhaName, Long messengerGroupId,
            List<ResourceTbl> resourceTbls,
            List<MessengerTbl> messengerTbls) {
        logger.info("[[mha={}]] try remove messengers {}", mhaName, messengerIpsToBeRemoved);
        List<String> messengerInstancesRemoved = Lists.newArrayList();
        if (messengerIpsToBeRemoved.size() != 0) {
            for (String ip : messengerIpsToBeRemoved) {
                logger.info("[[mha={}]]remove messenger: {}", mhaName, ip);
                ResourceTbl resourceTbl = resourceTbls.stream().filter(p -> ip.equalsIgnoreCase(p.getIp())).findFirst().orElse(null);
                if (null == resourceTbl) {
                    logger.info("[[mha={}]]UNLIKELY-messenger resource({}) should already be loaded", mhaName, ip);
                    continue;
                }
                MessengerTbl messengerTbl = messengerTbls.stream()
                        .filter(p -> (messengerGroupId.equals(p.getMessengerGroupId()))
                                && resourceTbl.getId().equals(p.getResourceId()))
                        .findFirst().orElse(null);
                try {
                    assert null != messengerTbl;
                    messengerTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    messengerTblDao.update(messengerTbl);
                    messengerInstancesRemoved.add(ip);
                } catch (Throwable t) {
                    logger.error("[[mha={}]]Failed remove messenger {}", mhaName, ip, t);
                }
            }
        }
        return messengerInstancesRemoved;
    }

    protected List<List<String>> getRemoveAndAddInstanceIps(List<String> ipsInUse, List<String> ipsNewConfigured) {
        List<List<String>> addRemoveReplicatorIpsPair = Lists.newArrayList();

        List<String> toBeAdded = Lists.newArrayList(ipsNewConfigured);
        toBeAdded.removeAll(Lists.newArrayList(ipsInUse));
        addRemoveReplicatorIpsPair.add(toBeAdded);

        List<String> toBeRemoved = Lists.newArrayList(ipsInUse);
        toBeRemoved.removeAll(Lists.newArrayList(ipsNewConfigured));
        addRemoveReplicatorIpsPair.add(toBeRemoved);

        return addRemoveReplicatorIpsPair;
    }

    private MachineTbl extractFrom(MachineDto machineDto, Long mhaId) throws Exception {
        boolean isMaster = Boolean.TRUE.equals(machineDto.getMaster());
        String uuid = MySqlUtils.getUuid(machineDto.getIp(), machineDto.getPort(),
                monitorTableSourceProvider.getMonitorUserVal(), monitorTableSourceProvider.getMonitorPasswordVal(),
                isMaster);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMhaId(mhaId);
        machineTbl.setMaster(isMaster ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode());
        machineTbl.setIp(machineDto.getIp());
        machineTbl.setPort(machineDto.getPort());
        machineTbl.setUuid(uuid);
        return machineTbl;
    }

    private MachineTbl extractFrom(MemberInfo memberInfo, Long mhaId) {
        String serviceIp = memberInfo.getService_ip();
        int dnsPort = memberInfo.getDns_port();
        String dcInDbaSystem = memberInfo.getMachine_located_short();
        boolean isMaster = memberInfo.getRole().toLowerCase().contains("master");
        String uuid = null;
        try {
            uuid = MySqlUtils.getUuid(serviceIp, dnsPort, monitorTableSourceProvider.getMonitorUserVal(),
                    monitorTableSourceProvider.getMonitorPasswordVal(), isMaster);
        } catch (Exception e) {
            logger.warn("getUuid failed, serviceIp:{}, dnsPort:{}, dcInDbaSystem:{}, isMaster:{}, e:{}",
                    serviceIp, dnsPort, dcInDbaSystem, isMaster, e.getMessage());
        }
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMhaId(mhaId);
        machineTbl.setMaster(isMaster ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode());
        machineTbl.setIp(serviceIp);
        machineTbl.setPort(dnsPort);
        machineTbl.setUuid(uuid);
        return machineTbl;
    }

    private RowsFilterConfigView buildRowsFilterConfigView(RowsFilterTblV2 rowsFilterTblV2) {
        RowsFilterConfigView rowsFilterConfigView = new RowsFilterConfigView();
        rowsFilterConfigView.setMode(rowsFilterTblV2.getMode());

        RowsFilterConfig.Configs configs = JsonUtils.fromJson(rowsFilterTblV2.getConfigs(), RowsFilterConfig.Configs.class);
        rowsFilterConfigView.setDrcStrategyId(configs.getDrcStrategyId());
        rowsFilterConfigView.setRouteStrategyId(configs.getRouteStrategyId());

        List<RowsFilterConfig.Parameters> parametersList = configs.getParameterList();
        RowsFilterConfig.Parameters firstParameters = parametersList.get(0);
        if (rowsFilterTblV2.getMode().equals(RowsFilterModeEnum.TRIP_UDL.getCode())) {
            if (firstParameters.getUserFilterMode().equals(UserFilterMode.Udl.getName())) {
                rowsFilterConfigView.setUdlColumns(firstParameters.getColumns());
            } else if (firstParameters.getUserFilterMode().equals(UserFilterMode.Uid.getName())) {
                rowsFilterConfigView.setColumns(firstParameters.getColumns());
            }
            if (parametersList.size() > 1) {
                RowsFilterConfig.Parameters secondParameters = parametersList.get(1);
                if (secondParameters.getUserFilterMode().equals(UserFilterMode.Udl.getName())) {
                    rowsFilterConfigView.setUdlColumns(secondParameters.getColumns());
                } else if (secondParameters.getUserFilterMode().equals(UserFilterMode.Uid.getName())) {
                    rowsFilterConfigView.setColumns(secondParameters.getColumns());
                }
            }
        } else {
            rowsFilterConfigView.setColumns(firstParameters.getColumns());
        }

        rowsFilterConfigView.setContext(firstParameters.getContext());
        rowsFilterConfigView.setIllegalArgument(firstParameters.getIllegalArgument());
        rowsFilterConfigView.setFetchMode(firstParameters.getFetchMode());

        return rowsFilterConfigView;
    }

    private ColumnsConfigView buildColumnsConfigView(ColumnsFilterTblV2 columnsFilterTblV2) {
        ColumnsConfigView columnsConfigView = new ColumnsConfigView();
        columnsConfigView.setColumns(JsonUtils.fromJsonToList(columnsFilterTblV2.getColumns(), String.class));
        columnsConfigView.setMode(columnsFilterTblV2.getMode());
        return columnsConfigView;
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

    private List<DbReplicationTbl> insertDbReplications(MhaTblV2 srcMha, long dstMhaId, Pair<List<String>, List<String>> dbTablePair, String nameFilter) throws Exception {
        List<String> dbList = dbTablePair.getLeft();
        List<String> tableList = dbTablePair.getRight();

        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
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

        checkExistDbReplication(tableList, new ArrayList<>(), srcMhaDbMappings, dstMhaDbMappings);

        dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        logger.info("insertDbReplications size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);
        return dbReplicationTbls;
    }

    private void checkExistDbReplication(List<String> tableList,
                                         List<Long> excludeDbReplicationIds,
                                         List<MhaDbMappingTbl> srcMhaDbMappings,
                                         List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        if (CollectionUtils.isEmpty(tableList)) {
            throw ConsoleExceptionUtils.message("cannot match any tables!");
        }

        List<Long> srcDbIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> srcDbTbls = dbTblDao.queryByIds(srcDbIds);
        Map<Long, String> srcDbMap = srcDbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, Long> srcDbMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        List<DbReplicationTbl> existDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings)
                .stream()
                .filter(e -> !excludeDbReplicationIds.contains(e.getId()))
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(existDbReplications)) {   //check contain same table
            String allNameFilter = buildNameFilterByDbReplications(existDbReplications, srcDbMappingMap, srcDbMap);
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(allNameFilter);
            List<String> existTableList = tableList.stream().filter(aviatorRegexFilter::filter).collect(Collectors.toList());

            if (!CollectionUtils.isEmpty(existTableList)) {
                throw ConsoleExceptionUtils.message(String.format("tables: %s has already been configured", existTableList));
            }
        }
    }

    private String buildNameFilterByDbReplications(List<DbReplicationTbl> dbReplicationTbls, Map<Long, Long> mhaDbMappingMap, Map<Long, String> dbMap) {
        StringBuilder nameFilterBuilder = new StringBuilder();
        int size = dbReplicationTbls.size();
        for (int i = 0; i < size; i++) {
            DbReplicationTbl dbReplicationTbl = dbReplicationTbls.get(i);
            long dbId = mhaDbMappingMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
            String dbName = dbMap.get(dbId);
            String nameFilter = dbName + "\\." + dbReplicationTbl.getSrcLogicTableName();
            nameFilterBuilder.append(nameFilter);

            if (i != size - 1) {
                nameFilterBuilder.append(",");
            }
        }

        return nameFilterBuilder.toString();
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

    private void configureApplierGroup(long mhaReplicationId, String applierInitGtid, List<String> applierIps, List<ResourceTbl> resourceTbls, List<DbReplicationTbl> dbReplications) throws Exception {
        long applierGroupId = insertOrUpdateApplierGroup(mhaReplicationId, applierInitGtid);
        configureAppliers(applierGroupId, applierIps, resourceTbls, dbReplications);
    }

    private void configureAppliers(long applierGroupId, List<String> applierIps, List<ResourceTbl> resourceTbls, List<DbReplicationTbl> dbReplications) throws Exception {
        if (CollectionUtils.isEmpty(dbReplications) && !CollectionUtils.isEmpty(applierIps)) {
            throw ConsoleExceptionUtils.message("dbReplication not config yet, cannot config applier");
        }

        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ApplierTblV2> existAppliers = applierTblDao.queryByApplierGroupId(applierGroupId, BooleanEnum.FALSE.getCode());
        Map<Long, ApplierTblV2> existApplierMap = existAppliers.stream().collect(Collectors.toMap(ApplierTblV2::getResourceId, Function.identity()));
        List<Long> existResourceIds = existAppliers.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
        List<String> existIps = resourceTbls.stream().filter(e -> existResourceIds.contains(e.getId())).map(ResourceTbl::getIp).collect(Collectors.toList());

        Pair<List<String>, List<String>> ipPairs = getAddAndDeleteResourceIps(applierIps, existIps);
        List<String> insertIps = ipPairs.getLeft();
        List<String> deleteIps = ipPairs.getRight();
        List<ApplierTblV2> insertAppliers = new ArrayList<>();
        List<ApplierTblV2> deleteAppliers = new ArrayList<>();

        if (!CollectionUtils.isEmpty(insertIps)) {
            for (String ip : insertIps) {
                ApplierTblV2 applierTbl = new ApplierTblV2();
                applierTbl.setApplierGroupId(applierGroupId);
                applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
                applierTbl.setMaster(BooleanEnum.FALSE.getCode());
                applierTbl.setResourceId(resourceTblMap.get(ip));
                applierTbl.setDeleted(BooleanEnum.FALSE.getCode());

                insertAppliers.add(applierTbl);
            }
            logger.info("insert insertIps: {}", insertIps);
            applierTblDao.batchInsert(insertAppliers);
        }

        if (!CollectionUtils.isEmpty(deleteIps)) {
            for (String ip : deleteIps) {
                ApplierTblV2 applierTbl = existApplierMap.get(resourceTblMap.get(ip));
                applierTbl.setDeleted(BooleanEnum.TRUE.getCode());
                deleteAppliers.add(applierTbl);
            }
            applierTblDao.update(deleteAppliers);
        }
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
            if (StringUtils.isNotBlank(applierInitGtid)) {
                existApplierGroup.setGtidInit(applierInitGtid);
            }
            existApplierGroup.setDeleted(BooleanEnum.FALSE.getCode());
            applierGroupTblDao.update(existApplierGroup);
        }
        return applierGroupId;
    }

    private void configureReplicatorGroup(MhaTblV2 mhaTblV2, String replicatorInitGtid, List<String> replicatorIps, List<ResourceTbl> resourceTbls) throws Exception {
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaTblV2.getId());
        configureReplicators(mhaTblV2.getMhaName(), replicatorGroupId, replicatorInitGtid, replicatorIps, resourceTbls);
    }

    private void configureReplicators(String mhaName, long replicatorGroupId, String replicatorInitGtid, List<String> replicatorIps, List<ResourceTbl> resourceTbls) throws Exception {
        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ReplicatorTbl> existReplicators = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupId), BooleanEnum.FALSE.getCode());
        Map<Long, ReplicatorTbl> existReplicatorMap = existReplicators.stream().collect(Collectors.toMap(ReplicatorTbl::getResourceId, Function.identity()));
        List<Long> existResourceIds = existReplicators.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
        List<String> existIps = resourceTbls.stream().filter(e -> existResourceIds.contains(e.getId())).map(ResourceTbl::getIp).collect(Collectors.toList());

        Pair<List<String>, List<String>> ipPairs = getAddAndDeleteResourceIps(replicatorIps, existIps);
        List<String> insertIps = ipPairs.getLeft();
        List<String> deleteIps = ipPairs.getRight();
        List<ReplicatorTbl> insertReplicators = new ArrayList<>();
        List<ReplicatorTbl> deleteReplicators = new ArrayList<>();

        if (!CollectionUtils.isEmpty(insertIps)) {
            for (String ip : insertIps) {
                ReplicatorTbl replicatorTbl = new ReplicatorTbl();
                replicatorTbl.setRelicatorGroupId(replicatorGroupId);
                String gtidInit = StringUtils.isNotBlank(replicatorInitGtid) ? formatGtid(replicatorInitGtid) : getNativeGtid(mhaName);
                replicatorTbl.setGtidInit(gtidInit);
                replicatorTbl.setResourceId(resourceTblMap.get(ip));
                replicatorTbl.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
                replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(ip));
                replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
                replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());

                insertReplicators.add(replicatorTbl);
            }
            logger.info("insert insertIps: {}", insertIps);
            replicatorTblDao.batchInsert(insertReplicators);
        }

        if (!CollectionUtils.isEmpty(deleteIps)) {
            for (String ip : deleteIps) {
                ReplicatorTbl replicatorTbl = existReplicatorMap.get(resourceTblMap.get(ip));
                replicatorTbl.setDeleted(BooleanEnum.TRUE.getCode());
                deleteReplicators.add(replicatorTbl);
            }
            replicatorTblDao.update(deleteReplicators);
        }
    }

    private Pair<List<String>, List<String>> getAddAndDeleteResourceIps(List<String> insertIps, List<String> existIps) {
        if (CollectionUtils.isEmpty(existIps) || CollectionUtils.isEmpty(insertIps)) {
            return Pair.of(insertIps, existIps);
        }
        List<String> addIps = new ArrayList<>();
        List<String> deleteIps = new ArrayList<>();
        for (String ip : insertIps) {
            if (!existIps.contains(ip)) {
                addIps.add(ip);
            }
        }

        for (String ip : existIps) {
            if (!insertIps.contains(ip)) {
                deleteIps.add(ip);
            }
        }

        return Pair.of(addIps, deleteIps);
    }

    private String formatGtid(String gtid) {
        if (gtid == null) {
            return null;
        }
        gtid = gtid.replace("#", "");
        return XmlUtils.replaceBlank(gtid);
    }

    private String getNativeGtid(String mhaName) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mhaName);
        return MySqlUtils.getExecutedGtid(endpoint);
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
            if (existReplicatorGroup.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
                existReplicatorGroup.setDeleted(BooleanEnum.FALSE.getCode());
                replicatorGroupTblDao.update(existReplicatorGroup);
            }
        }
        return replicatorGroupId;
    }

    private void insertMhaReplication(long srcMhaId, long dstMhaId) throws Exception {
        MhaReplicationTbl existMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId);
        if (existMhaReplication != null) {
            if (existMhaReplication.getDeleted().equals(BooleanEnum.FALSE.getCode())) {
                logger.info("mhaReplication already exist, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
                return;
            } else {
                existMhaReplication.setDeleted(BooleanEnum.FALSE.getCode());
                logger.info("recover mhaReplication, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
                mhaReplicationTblDao.update(existMhaReplication);
            }

        } else {
            MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
            mhaReplicationTbl.setSrcMhaId(srcMhaId);
            mhaReplicationTbl.setDstMhaId(dstMhaId);
            mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaReplicationTbl.setDrcStatus(BooleanEnum.FALSE.getCode());

            logger.info("insertMhaReplication srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            mhaReplicationTblDao.insert(mhaReplicationTbl);
        }
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

    private long getBuId(String buName) throws Exception {
        BuTbl existBuTbl = buTblDao.queryByBuName(buName);
        if (existBuTbl != null) {
            return existBuTbl.getId();
        }

        BuTbl buTbl = new BuTbl();
        buTbl.setBuName(buName);
        buTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return buTblDao.insertWithReturnId(buTbl);
    }

    private MhaTblV2 buildMhaTbl(String mhaName, long dcId, long buId, String tag) {
        String clusterName = mhaName + CLUSTER_NAME_SUFFIX;
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName(mhaName);
        mhaTblV2.setDcId(dcId);
        mhaTblV2.setApplyMode(ApplyMode.transaction_table.getType());
        mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
        mhaTblV2.setBuId(buId);
        mhaTblV2.setClusterName(clusterName);
        mhaTblV2.setAppId(-1L);
        mhaTblV2.setDeleted(BooleanEnum.FALSE.getCode());
        mhaTblV2.setTag(tag);

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
        param.setSrcMhaName(param.getSrcMhaName().trim());
        param.setDstMhaName(param.getDstMhaName().trim());
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkArgument(!param.getSrcMhaName().equals(param.getDstMhaName()), "srcMha and dstMha cannot be same!");
        PreconditionUtils.checkString(param.getBuName(), "buName requires not null!");
        PreconditionUtils.checkString(param.getSrcDc(), "srcDc requires not null!");
        PreconditionUtils.checkString(param.getDstDc(), "dstDcId requires not null!");
    }

    private void checkMessengerMhaBuildParam(MessengerMhaBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getMhaName(), "mhaName requires not empty!");
        PreconditionUtils.checkString(param.getDc(), "dcName requires not empty!");
        PreconditionUtils.checkString(param.getBuName(), "buName requires not null!");
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
