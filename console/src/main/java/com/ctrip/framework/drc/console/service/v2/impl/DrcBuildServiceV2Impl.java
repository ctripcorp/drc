package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.v2.MachineDto;
import com.ctrip.framework.drc.console.dto.v3.DbApplierSwitchReqDto;
import com.ctrip.framework.drc.console.dto.v3.MessengerSwitchReqDto;
import com.ctrip.framework.drc.console.dto.v3.ReplicatorInfoDto;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.enums.v2.EffectiveStatusEnum;
import com.ctrip.framework.drc.console.enums.v2.ExistingDataStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.mysql.DrcMessengerGtidTblCreateReq;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.MemberInfo;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.service.v2.security.MetaAccountService;
import com.ctrip.framework.drc.console.utils.*;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
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
    private ReplicationTableTblDao replicationTableTblDao;
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
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private AccountService accountService;
    @Autowired
    private KmsService kmsService;
    @Autowired
    private MetaAccountService metaAccountService;
    @Autowired
    private DbDrcBuildService dbDrcBuildService;

    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "drcMetaRefreshV2");
    private final ListeningExecutorService replicationExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(20, "replicationExecutorService"));

    private static final String CLUSTER_NAME_SUFFIX = "_dalcluster";
    private static final String DEFAULT_TABLE_NAME = ".*";

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Pair<Long,Long> mhaInitBeforeBuildIfNeed(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);
        
        long buId = getBuId(param.getBuName());
        DcTbl srcDcTbl = dcTblDao.queryByDcName(param.getSrcDc());
        DcTbl dstDcTbl = dcTblDao.queryByDcName(param.getDstDc());
        if (srcDcTbl == null || dstDcTbl == null) {
            throw ConsoleExceptionUtils.message(String.format("srcDc: %s or dstDc: %s not exist", param.getSrcDc(), param.getDstDc()));
        }

        MhaTblV2 srcMha = buildMhaTbl(param.getSrcMhaName(), srcDcTbl.getId(), buId, param.getSrcTag());
        MhaTblV2 dstMha = buildMhaTbl(param.getDstMhaName(), dstDcTbl.getId(), buId, param.getDstTag());

        long srcMhaId = initMhaAndAccount(srcMha,param.getSrcMachines());
        long dstMhaId = initMhaAndAccount(dstMha,param.getDstMachines());
        return Pair.of(srcMhaId,dstMhaId);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMhaAndReplication(DrcMhaBuildParam param) throws Exception {
        Pair<Long,Long> mhaIds = mhaInitBeforeBuildIfNeed(param);
        long srcMhaId = mhaIds.getLeft();
        long dstMhaId = mhaIds.getRight();
        
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
        long mhaId = initMhaAndAccount(mhaTbl, param.getMachineDto());

        // messengerGroup
        Long srcReplicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(mhaId);
        mysqlServiceV2.createDrcMessengerGtidTbl(new DrcMessengerGtidTblCreateReq(mhaTbl.getMhaName()));
        messengerGroupTblDao.upsertIfNotExist(mhaId, srcReplicatorGroupId, "", MqType.parse(param.getMqType()));
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
        Pair<List<DbReplicationTbl>, Map<String, Long>> pair = insertDbReplications(srcMha, dstMha.getId(), dbTablePair, nameFilter);
        List<DbReplicationTbl> dbReplicationTbls = pair.getLeft();
        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());

        Map<String, Long> dbNameToSrcMhaDbMappingId = pair.getRight();
        //insert replicationTables
        configReplicationTables(param, dbReplicationTbls, dbNameToSrcMhaDbMappingId, dbTablePair.getRight(), false);
        return dbReplicationIds;
    }

    private void addConflictBlackList(String nameFilter) {
        executorService.submit(() -> {
            try {
                conflictLogService.addDbBlacklist(nameFilter, CflBlacklistType.NEW_CONFIG,null);
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
        Map<String, Long> dbNameToSrcMhaDbMappingId = checkExistDbReplication(tableList, dbReplicationIds, srcMhaDbMappings, dstMhaDbMappings);

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
            RowsFilterConfigView rowsConfigView = RowsFilterConfigView.from(rowsFilterTblV2);
            if (!CollectionUtils.isEmpty(rowsConfigView.getColumns())) {
                filterColumns.addAll(rowsConfigView.getColumns());
            }
            if (!CollectionUtils.isEmpty(rowsConfigView.getUdlColumns())) {
                filterColumns.addAll(rowsConfigView.getUdlColumns());
            }
        }

        if (columnsFilterId != -1L) {
            ColumnsFilterTblV2 columnsFilterTblV2 = columnFilterTblV2Dao.queryById(columnsFilterId);
            ColumnsConfigView columnsConfigView = ColumnsConfigView.from(columnsFilterTblV2);
            filterColumns.addAll(columnsConfigView.getColumns());
        }

        Set<String> commonColumns = mysqlServiceV2.getCommonColumnIn(param.getSrcMhaName(), param.getDbName(), param.getTableName());
        if (!commonColumns.containsAll(filterColumns)) {
            throw ConsoleExceptionUtils.message("rowsFilter or columnsFilter columns not match");
        }

        //insert replicationTables
        configReplicationTables(param, dbReplicationTbls, dbNameToSrcMhaDbMappingId, tableList, true);
        return dbReplicationIds;
    }

    private void configReplicationTables(DbReplicationBuildParam param, List<DbReplicationTbl> dbReplicationTbls, Map<String, Long> dbNameToSrcMhaDbMappingId, List<String> tableList, boolean update) throws SQLException {
        String srcMhaName = param.getSrcMhaName();
        String dstMhaName = param.getDstMhaName();
        Map<Long, Long> dbReplicationMappingMap = dbReplicationTbls.stream().collect(Collectors.toMap(DbReplicationTbl::getSrcMhaDbMappingId, DbReplicationTbl::getId));
        String srcRegion = mhaServiceV2.getRegion(srcMhaName);
        String dstRegion = mhaServiceV2.getRegion(dstMhaName);
        List<ReplicationTableTbl> replicationTableTbls = new ArrayList<>();
        for (String table : tableList) {
            String[] tables = table.split("\\.");
            String dbName = tables[0];
            String tableName = tables[1];
            long dbReplicationId = dbReplicationMappingMap.get(dbNameToSrcMhaDbMappingId.get(dbName));

            ReplicationTableTbl replicationTableTbl = new ReplicationTableTbl();
            replicationTableTbl.setDbReplicationId(dbReplicationId);
            replicationTableTbl.setDbName(dbName);
            replicationTableTbl.setTableName(tableName);
            replicationTableTbl.setSrcMha(srcMhaName);
            replicationTableTbl.setDstMha(dstMhaName);
            replicationTableTbl.setSrcRegion(srcRegion);
            replicationTableTbl.setDstRegion(dstRegion);
            replicationTableTbl.setDeleted(BooleanEnum.FALSE.getCode());
            //ql_deng TODO 2024/2/28: EFFECTIVE -> NOT_IN_EFFECT
            if (param.isAutoBuild()) {
                replicationTableTbl.setEffectiveStatus(EffectiveStatusEnum.IN_EFFECT.getCode());
            } else {
                replicationTableTbl.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode());
            }


            replicationTableTbl.setExistingDataStatus(param.isFlushExistingData() ?
                    ExistingDataStatusEnum.NOT_PROCESSED.getCode() : ExistingDataStatusEnum.NO_NEED_TO_PROCESSING.getCode());
            replicationTableTbls.add(replicationTableTbl);
        }

        List<ReplicationTableTbl> existReplicationTables = new ArrayList<>();
        if (update) {
            existReplicationTables = replicationTableTblDao.queryByDbReplicationIds(param.getDbReplicationIds(), BooleanEnum.FALSE.getCode());
        }
        Set<ReplicationTableTbl> existReplicationTableSet = Sets.newHashSet(existReplicationTables);
        Set<ReplicationTableTbl> newReplicationTableTblSet = Sets.newHashSet(replicationTableTbls);
        List<ReplicationTableTbl> insertReplicationTables = replicationTableTbls.stream().filter(e -> !existReplicationTableSet.contains(e)).collect(Collectors.toList());
        List<ReplicationTableTbl> deleteReplicationTables = existReplicationTables.stream().filter(e -> !newReplicationTableTblSet.contains(e)).collect(Collectors.toList());
        //ql_deng TODO 2024/2/29: EFFECTIVE -> NOT_IN_EFFECT
        deleteReplicationTables.stream().forEach(e -> {
            e.setDeleted(BooleanEnum.TRUE.getCode());
            e.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode());
        });

        if (!CollectionUtils.isEmpty(insertReplicationTables)) {
            logger.info("{} -> {} insert replicationTables", srcMhaName, dstMhaName);
            replicationTableTblDao.insert(insertReplicationTables);
        }
        if (!CollectionUtils.isEmpty(deleteReplicationTables)) {
            logger.info("{} -> {} delete replicationTables", srcMhaName, dstMhaName);
            replicationTableTblDao.update(deleteReplicationTables);
        }
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
        mhaDbReplicationService.offlineMhaDbReplication(dbReplicationTbls);

        //config replicationTables
        List<ReplicationTableTbl> replicationTableTbls = replicationTableTblDao.queryByDbReplicationIds(dbReplicationIds, BooleanEnum.FALSE.getCode());
        //ql_deng TODO 2024/3/1:EFFECTIVE -> NOT_IN_EFFECT
        replicationTableTbls.stream().forEach(e -> {
            e.setDeleted(BooleanEnum.TRUE.getCode());
            e.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode());
        });
        replicationTableTblDao.update(replicationTableTbls);

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
        return ColumnsConfigView.from(columnsFilterTblV2);
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
        return RowsFilterConfigView.from(rowsFilterTblV2);
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
    public String buildMessengerDrc(MessengerMetaDto dto) throws Exception {
        this.doBuildMessengerDrc(dto);
        Drc drcMessengerConfig = metaInfoService.getDrcMessengerConfig(dto.getMhaName(), MqType.parseOrDefault(dto.getMqType()));
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProviderV2.scheduledTask error. req: " + dto, e);
        }
        return XmlUtils.formatXML(drcMessengerConfig.toString());
    }
    
    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public MhaTblV2 syncMhaInfoFormDbaApi(String newMha, String oldMha) throws SQLException {
        MhaTblV2 newMhaTbl = mhaTblDao.queryByMhaName(newMha,BooleanEnum.FALSE.getCode());
        if (newMhaTbl != null) {
            throw ConsoleExceptionUtils.message(newMha + "mhaName already exist!");
        }
        DbaClusterInfoResponse clusterMembersInfo = dbaApiService.getClusterMembersInfo(newMha);
        List<MemberInfo> memberlist = clusterMembersInfo.getData().getMemberlist();
        String dcInDbaSystem = memberlist.stream().findFirst().map(MemberInfo::getMachine_located_short).get();
        Map<String, String> dbaDc2DrcDcMap = consoleConfig.getDbaDc2DrcDcMap();
        String dcInDrc = dbaDc2DrcDcMap.getOrDefault(dcInDbaSystem.toLowerCase(), null);
        DcTbl dcTbl = dcTblDao.queryByDcName(dcInDrc);
        // record mha and accountv2
        MhaTblV2 mhaTobeInit = buildMhaTbl(newMha, dcTbl.getId(), 1L, ResourceTagEnum.COMMON.getName());
        Long mhaId;
        if (StringUtils.isNotBlank(oldMha)) {
            MhaTblV2 oldMhaTbl = mhaTblDao.queryByMhaName(oldMha,0);
            if(oldMhaTbl == null) {
                throw ConsoleExceptionUtils.message(oldMha + "mha not exist,can't copyMhaProperties");
            }
            copyMhaProperties(mhaTobeInit,oldMhaTbl);
            MhaTblV2 sameNameMhaAsNewMhaButDeleted = mhaTblDao.queryByMhaName(newMha, BooleanEnum.TRUE.getCode());
            if (sameNameMhaAsNewMhaButDeleted != null) {
                mhaId = sameNameMhaAsNewMhaButDeleted.getId();
                mhaTobeInit.setId(mhaId);
                mhaTblDao.update(mhaTobeInit);
            } else {
                mhaId = mhaTblDao.insertWithReturnId(mhaTobeInit);
            }
            logger.info("[[mha={}]] syncMhaInfoFormDbaApi mhaTbl affect mhaId:{},copyMhaProperties from:{}", newMha, mhaId,oldMha);
        } else {
            mhaId = initMhaAndAccount(mhaTobeInit, memberlist.stream().map(MemberInfo::toMachineDto).collect(Collectors.toList()));
            logger.info("[[mha={}]] syncMhaInfoFormDbaApi mhaTbl affect mhaId:{}", newMha, mhaId);
        }
        
        // record machines
        List<MachineTbl> machinesToBeInsert = new ArrayList<>();
        for (MemberInfo memberInfo : memberlist) {
            machinesToBeInsert.add(extractFrom(memberInfo, mhaId, newMha));
        }
        int[] ints = machineTblDao.batchInsert(machinesToBeInsert);
        logger.info("[[mha={}]] syncMhaInfoFormDbaApi machineTbl affect rows:{}", newMha, Arrays.stream(ints).sum());
        mhaTobeInit.setId(mhaId);
        return mhaTobeInit;
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
            MachineTbl machineTbl = extractFrom(memberInfo, mhaId,mhaName);
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
    public void autoConfigMessengersWithRealTimeGtid(MhaTblV2 mhaTbl, MqType mqType, boolean switchOnly) throws SQLException {
        String mhaExecutedGtid = mysqlServiceV2.getMhaExecutedGtid(mhaTbl.getMhaName());
        if (StringUtils.isBlank(mhaExecutedGtid)) {
            logger.error("[[mha={},mqType={}]] getMhaExecutedGtid failed", mhaTbl.getMhaName(), mqType.name());
            throw new ConsoleException("getMhaExecutedGtid failed!");
        }

        autoConfigMessenger(mhaTbl, mhaExecutedGtid, mqType, switchOnly);
    }

    @Deprecated
    @Override
    public void autoConfigMessengersWithRealTimeGtid(MhaTblV2 mhaTbl,boolean switchOnly) throws SQLException {
        autoConfigMessengersWithRealTimeGtid(mhaTbl, MqType.DEFAULT, switchOnly);
    }

    @Override
    public void autoConfigMessenger(MhaTblV2 mhaTbl, String gtid, MqType mqType, boolean switchOnly) throws SQLException {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaTbl.getId(), mqType, BooleanEnum.FALSE.getCode());
        // messengers
        List<MessengerTbl> existMessengers = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (switchOnly && CollectionUtils.isEmpty(existMessengers)) {
            logger.info("[[mha={},mqType={}]] messengers not exist,do nothing when switchOnly", mhaTbl.getMhaName(), mqType.name());
            return;
        }

        if (StringUtils.isBlank(messengerGroupTbl.getGtidExecuted()) && StringUtils.isBlank(gtid)) {
            throw ConsoleExceptionUtils.message(String.format("configure messenger fail, init gtid needed! mha: %s, mqType: %s", mhaTbl.getMhaName(), mqType.name()));
        }
        if (!StringUtils.isBlank(gtid) && !gtid.equals(messengerGroupTbl.getGtidExecuted())) {
            messengerGroupTbl.setGtidExecuted(gtid);
            messengerGroupTblDao.update(messengerGroupTbl);
            logger.info("[[mha={},mqType={}]] autoConfigMessengersWithRealTimeGtid with gtid:{}", mhaTbl.getMhaName(), mqType.name(), gtid);
        }

        List<Long> inUseResourceId = existMessengers.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
        List<String> inUseIps = resourceTblDao.queryByIds(inUseResourceId).stream().map(ResourceTbl::getIp).collect(Collectors.toList());

        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.MESSENGER.getCode());
        selectParam.setMhaName(mhaTbl.getMhaName());
        selectParam.setSelectedIps(inUseIps);
        List<ResourceView> resourceViews = resourceService.handOffResource(selectParam);
        if (CollectionUtils.isEmpty(resourceViews)) {
            logger.error("[[mha={}]] autoConfigMessengers failed", mhaTbl.getMhaName());
            throw new ConsoleException("autoConfigMessengers failed!");
        }
        // insert new messengers
        List<MessengerTbl> insertMessengers = resourceViews.stream()
                .filter(e -> !inUseResourceId.contains(e.getResourceId()))
                .map(e -> buildMessengerTbl(messengerGroupTbl, e))
                .collect(Collectors.toList());

        // delete old messengers
        List<Long> newResourceId = resourceViews.stream().map(ResourceView::getResourceId).collect(Collectors.toList());
        List<MessengerTbl> deleteMessengers = existMessengers.stream()
                .filter(e -> !newResourceId.contains(e.getResourceId()))
                .collect(Collectors.toList());
        deleteMessengers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));

        messengerTblDao.batchInsert(insertMessengers);
        messengerTblDao.batchUpdate(deleteMessengers);
        logger.info("[[mha={},mqType={}]] autoConfigMessengers success", mhaTbl.getMhaName(), mqType.name());
    }

    @Override
    public void autoConfigMessenger(MhaTblV2 mhaTbl, String gtid, boolean switchOnly) throws SQLException {
        autoConfigMessenger(mhaTbl, gtid, MqType.DEFAULT, switchOnly);
    }

    private static MessengerTbl buildMessengerTbl(MessengerGroupTbl messengerGroupTbl, ResourceView resourceView) {
        MessengerTbl messengerTbl = new MessengerTbl();
        messengerTbl.setMessengerGroupId(messengerGroupTbl.getId());
        messengerTbl.setResourceId(resourceView.getResourceId());
        messengerTbl.setPort(DEFAULT_APPLIER_PORT);
        messengerTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return messengerTbl;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void initReplicationTables() throws Exception {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAllExist().stream().filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblDao.queryAllExist();
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryAllExist();
        List<DbTbl> dbTbls = dbTblDao.queryAllExist();
        Map<Long, String> dbNameMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, List<MhaDbMappingTbl>> mhaIdToMappingMap = mhaDbMappingTbls.stream().collect(Collectors.groupingBy(MhaDbMappingTbl::getMhaId));
        Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getRegionName));
        Map<Long, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity()));

        List<ListenableFuture<List<ReplicationTableTbl>>> futures = new ArrayList<>();
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            ListenableFuture<List<ReplicationTableTbl>> future = replicationExecutorService.submit(() -> buildReplicationTables(dbNameMap, mhaIdToMappingMap, mhaDbMappingTblMap, dcMap, mhaMap, mhaReplicationTbl));
            futures.add(future);
        }

        List<ReplicationTableTbl> replicationTableTbls = new ArrayList<>();
        for (ListenableFuture<List<ReplicationTableTbl>> future : futures) {
            try {
                List<ReplicationTableTbl> result = future.get(10, TimeUnit.SECONDS);
                replicationTableTbls.addAll(result);
            } catch (Exception e) {
                throw ConsoleExceptionUtils.message("initReplicationTables fail, " + e);
            }
        }
    }

    /**
     * 灰度阶段使用
     * 物理删除，谨慎操作
     * @throws Exception
     */
    @Override
    public void deleteAllReplicationTables() throws Exception {
        replicationTableTblDao.deleteAll();
    }

    private List<ReplicationTableTbl> buildReplicationTables(Map<Long, String> dbNameMap, Map<Long, List<MhaDbMappingTbl>> mhaIdToMappingMap, Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap, Map<Long, String> dcMap, Map<Long, MhaTblV2> mhaMap, MhaReplicationTbl mhaReplicationTbl) throws Exception {
        MhaTblV2 srcMha = mhaMap.get(mhaReplicationTbl.getSrcMhaId());
        MhaTblV2 dstMha = mhaMap.get(mhaReplicationTbl.getDstMhaId());
        List<MhaDbMappingTbl> srcMhaMappings = mhaIdToMappingMap.get(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaMappings = mhaIdToMappingMap.get(dstMha.getId());
        List<DbReplicationTbl> dbReplicationTbls = getExistDbReplications(srcMhaMappings, dstMhaMappings);

        List<String> tableFilters = dbReplicationTbls.stream().map(dbReplicationTbl -> {
            String dbName = dbNameMap.get(mhaDbMappingTblMap.get(dbReplicationTbl.getSrcMhaDbMappingId()).getDbId());
            return dbName + "\\." + dbReplicationTbl.getSrcLogicTableName();
        }).collect(Collectors.toList());
        List<String> tableLists = null;
        String tableFilter = Joiner.on(",").join(tableFilters);
        try {
            tableLists = mysqlServiceV2.queryTablesWithNameFilter(srcMha.getMhaName(), tableFilter);
        } catch (Exception e) {
            logger.error("queryTablesWithNameFilter error, mhaName: {}, tableFilter: {}", srcMha.getMhaName(), tableFilter);
            throw ConsoleExceptionUtils.message(String.format("queryTablesWithNameFilter error, mhaName: %s, tableFilter: %s", srcMha.getMhaName(), tableFilter));
        }

        List<ReplicationTableTbl> replicationTableTbls = tableLists.stream().map(source -> {
            String[] tableStr = source.split("\\.");
            String dbName = tableStr[0];
            String tableName = tableStr[1];

            ReplicationTableTbl target = new ReplicationTableTbl();
            target.setDbName(dbName);
            target.setTableName(tableName);
            target.setSrcMha(srcMha.getMhaName());
            target.setDstMha(dstMha.getMhaName());
            target.setSrcRegion(dcMap.get(srcMha.getDcId()));
            target.setDstRegion(dcMap.get(dstMha.getDcId()));
            target.setExistingDataStatus(ExistingDataStatusEnum.PROCESSING_COMPLETED.getCode());
            target.setEffectiveStatus(EffectiveStatusEnum.EFFECTIVE.getCode());

            target.setDbReplicationId(getDbReplicationIdByTable(dbReplicationTbls, dbNameMap, mhaDbMappingTblMap, dbName, tableName));
            return target;
        }).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(replicationTableTbls)) {
            replicationTableTblDao.insert(replicationTableTbls);
        }
        return replicationTableTbls;
    }

    private long getDbReplicationIdByTable(List<DbReplicationTbl> dbReplicationTbls, Map<Long, String> dbNameMap, Map<Long, MhaDbMappingTbl> mhaDbMappingTblMap, String dbName, String tableName) {
        for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
            String db = dbNameMap.get(mhaDbMappingTblMap.get(dbReplicationTbl.getSrcMhaDbMappingId()).getDbId());
            if (!db.equalsIgnoreCase(dbName)) {
                continue;
            }
            AviatorRegexFilter filter = new AviatorRegexFilter(dbReplicationTbl.getSrcLogicTableName());
            if (filter.filter(tableName)) {
                return dbReplicationTbl.getId();
            }
        }
        throw ConsoleExceptionUtils.message("not find dbReplication tableName: " + dbName + "." + tableName);
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void doBuildMessengerDrc(MessengerMetaDto dto) throws Exception {

        // 0. check
        MhaTblV2 mhaTbl = mhaTblDao.queryByMhaName(dto.getMhaName(), BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not recorded");
        }
        if (!CollectionUtils.isEmpty(dto.getMessengerIps())) {
            List<MqConfigVo> mqConfigVos = messengerServiceV2.queryMhaMessengerConfigs(dto.getMhaName(), MqType.parse(dto.getMqType()));
            if (CollectionUtils.isEmpty(mqConfigVos)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "Add mq config before put messengers!");
            }
        }
        // 1. configure and persistent in database
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaTbl.getId());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        configureReplicators(mhaTbl.getMhaName(), replicatorGroupId, dto.getrGtidExecuted(), dto.getReplicatorIps(), resourceTbls);
        configureMessengers(mhaTbl, MqType.parse(dto.getMqType()), replicatorGroupId, dto.getMessengerIps(), dto.getaGtidExecuted());
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void doConfigMhaReplicator(MessengerMetaDto dto) throws Exception {
        // 0. check
        MhaTblV2 mhaTbl = mhaTblDao.queryByMhaName(dto.getMhaName(), BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not recorded");
        }
        // 1. configure and persistent in database
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaTbl.getId());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        configureReplicators(mhaTbl.getMhaName(), replicatorGroupId, dto.getrGtidExecuted(), dto.getReplicatorIps(), resourceTbls);
    }

    public Long configureMessengers(MhaTblV2 mhaTbl,
                                    MqType mqType, Long replicatorGroupId,
                                    List<String> messengerIps,
                                    String gtidExecuted) throws SQLException {
        Long messengerGroupId = configureMessengerGroup(mhaTbl,mqType, replicatorGroupId, gtidExecuted);
        configureMessengerInstances(mhaTbl, messengerIps, messengerGroupId);
        return messengerGroupId;
    }

    protected Long configureMessengerGroup(MhaTblV2 mhaTbl, MqType mqType, Long replicatorGroupId, String gtidExecuted) throws SQLException {
        String mhaName = mhaTbl.getMhaName();
        Long mhaId = mhaTbl.getId();
        logger.info("[[mha={}, mhaId={},replicatorGroupId={}]]configure or update messenger group", mhaName, mhaId, replicatorGroupId);
        mysqlServiceV2.createDrcMessengerGtidTbl(new DrcMessengerGtidTblCreateReq(mhaTbl.getMhaName()));
        return messengerGroupTblDao.upsertIfNotExist(mhaId, replicatorGroupId, formatGtid(gtidExecuted), mqType);
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

    private MachineTbl extractFrom(MachineDto machineDto, Long mhaId,String mhaName) throws Exception {
        boolean isMaster = Boolean.TRUE.equals(machineDto.getMaster());
        MhaAccounts mhaAccounts = metaAccountService.getMhaAccounts(mhaName);
        Account monitorAcc = mhaAccounts.getMonitorAcc();
        String uuid = MySqlUtils.getUuid(
                    machineDto.getIp(), machineDto.getPort(),
                    monitorAcc.getUser(), monitorAcc.getPassword(),
                    isMaster);
        MachineTbl machineTbl = new MachineTbl();
        machineTbl.setMhaId(mhaId);
        machineTbl.setMaster(isMaster ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode());
        machineTbl.setIp(machineDto.getIp());
        machineTbl.setPort(machineDto.getPort());
        machineTbl.setUuid(uuid);
        return machineTbl;
    }

    private MachineTbl extractFrom(MemberInfo memberInfo, Long mhaId,String mhaName) {
        String serviceIp = memberInfo.getService_ip();
        int dnsPort = memberInfo.getDns_port();
        String dcInDbaSystem = memberInfo.getMachine_located_short();
        boolean isMaster = memberInfo.getRole().toLowerCase().contains("master");
        String uuid = null;
        try {
            MhaAccounts mhaAccounts = metaAccountService.getMhaAccounts(mhaName);
            Account monitorAcc = mhaAccounts.getMonitorAcc();
             uuid = MySqlUtils.getUuid(serviceIp, dnsPort, monitorAcc.getUser(), monitorAcc.getPassword(), isMaster);
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

    private Pair<List<DbReplicationTbl>, Map<String, Long>> insertDbReplications(MhaTblV2 srcMha, long dstMhaId, Pair<List<String>, List<String>> dbTablePair, String nameFilter) throws Exception {
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

        Map<String, Long> mhaDbNameMap = checkExistDbReplication(tableList, new ArrayList<>(), srcMhaDbMappings, dstMhaDbMappings);

        List<DbReplicationTbl> insertDbReplicationTbls = dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        mhaDbReplicationService.maintainMhaDbReplication(dbReplicationTbls);
        logger.info("insertDbReplications size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);

        return Pair.of(insertDbReplicationTbls, mhaDbNameMap);
    }

    private Map<String, Long> checkExistDbReplication(List<String> tableList,
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

        Map<String, Long> dbNameToSrcMhaDbMappingId = new HashMap<>();
        srcDbMappingMap.forEach((srcMhaDbMappingId, dbId) -> {
            String dbName = srcDbMap.get(dbId);
            dbNameToSrcMhaDbMappingId.put(dbName, srcMhaDbMappingId);
        });
        return dbNameToSrcMhaDbMappingId;
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

    @Override
    public Long configureReplicatorGroup(MhaTblV2 mhaTblV2, String replicatorInitGtid, List<String> replicatorIps, List<ResourceTbl> resourceTbls) throws Exception {
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaTblV2.getId());
        configureReplicators(mhaTblV2.getMhaName(), replicatorGroupId, replicatorInitGtid, replicatorIps, resourceTbls);
        return replicatorGroupId;
    }

    @Override
    public String configReplicatorOnly(MessengerMetaDto dto) throws Exception {
        this.doConfigMhaReplicator(dto);
        Drc drcMessengerConfig = metaInfoService.getDrcMhaConfig(dto.getMhaName());
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProviderV2.scheduledTask error. req: " + dto, e);
        }
        return XmlUtils.formatXML(drcMessengerConfig.toString());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int isolationMigrateReplicator(List<String> mhas, boolean master, String tag, String gtid)
            throws SQLException {
        checkIsolationMigrateLimit(mhas);
        if (StringUtils.isNotBlank(gtid) && mhas.size() >1) {
            throw ConsoleExceptionUtils.message("gtid is not empty, mhas size must be 1");
        }
        int affectReplicator = 0;
        for (String mha : mhas) {
            MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mha);
            checkMhaTag(tag, mha, mhaTblV2);
            // current
            List<ReplicatorInfoDto> existReplicators = mhaServiceV2.getMhaReplicatorsV2(mha);
            ReplicatorInfoDto replicatorTobeReplaced = existReplicators.stream().filter(r -> r.getMaster() == master).findFirst().orElse(null);
            ReplicatorInfoDto anotherReplicator = existReplicators.stream().filter(r -> r.getMaster() != master).findFirst().orElse(null);
            if (replicatorTobeReplaced == null) {
                throw ConsoleExceptionUtils.message(mha + " replicator not found,master role: " + master);
            }
            if (anotherReplicator == null) {
                throw ConsoleExceptionUtils.message(mha + " another replicator not found,master role: " + !master);
            }
 
            // chosen
            List<ResourceView> replicatorForChosen = resourceService.getMhaAvailableResource(mha, ModuleEnum.REPLICATOR.getCode());
            ResourceView chosen = replicatorForChosen.stream().filter(
                    resource -> resource.getTag().equals(tag) && !resource.getAz().equals(anotherReplicator.getAz())
            ).findFirst().orElse(null);
            if (chosen == null) {
                throw ConsoleExceptionUtils.message(mha + "chosen isolationMigrateReplicator not found");
            }
            
            // replace the replicator's resourceId,
            String gtidInit = StringUtils.isNotBlank(gtid) ? gtid : mysqlServiceV2.getMhaExecutedGtid(mha);
            if (StringUtils.isBlank(gtidInit)) {
                throw ConsoleExceptionUtils.message(mha + " getMhaExecutedGtid empty");
            }
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            replicatorTbl.setId(replicatorTobeReplaced.getReplicatorId());
            replicatorTbl.setResourceId(chosen.getResourceId());
            replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(chosen.getIp()));
            replicatorTbl.setGtidInit(gtidInit);
            replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
            if (replicatorTblDao.update(replicatorTbl) != 1) {
                throw ConsoleExceptionUtils.message(mha + "replicator update failed " + replicatorTbl.getId());
            }
            affectReplicator++;
        }
        return affectReplicator;
    }
    

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public int isolationMigrateApplier(List<String> mhas, String tag) throws Exception {
        checkIsolationMigrateLimit(mhas);
        int affectMhas = 0;
        for (String mha : mhas) {
            MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mha);
            checkMhaTag(tag, mha, mhaTblV2);
            
            // switch current applier
            List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByDstMhaId(mhaTblV2.getId());
            List<DbApplierSwitchReqDto> switchReqDtos = Lists.newArrayList();
            for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
                MhaTblV2 srcMha = mhaTblDao.queryByPk(mhaReplicationTbl.getSrcMhaId());
                DbApplierSwitchReqDto switchReqDto = new DbApplierSwitchReqDto();
                switchReqDto.setSrcMhaName(srcMha.getMhaName());
                switchReqDto.setDstMhaName(mha);
                switchReqDto.setDbNames(null);
                switchReqDto.setSwitchOnly(true);
                switchReqDtos.add(switchReqDto);
            }
            dbDrcBuildService.switchAppliers(switchReqDtos);
            
            // switch current messenger
            for (MqType mqType : MqType.values()) {
                MhaTblV2 mhaTbl = mhaTblDao.queryByMhaName(mha, BooleanEnum.FALSE.getCode());
                MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaTbl.getId(), mqType,
                        BooleanEnum.FALSE.getCode());
                if (messengerGroupTbl != null) {
                    MessengerSwitchReqDto messengerSwitchReq = new MessengerSwitchReqDto();
                    messengerSwitchReq.setSrcMhaName(mha);
                    messengerSwitchReq.setSwitchOnly(true);
                    messengerSwitchReq.setMqType(mqType.name());
                    dbDrcBuildService.switchMessengers(Lists.newArrayList(messengerSwitchReq));
                }
            }

            affectMhas++;
        }
        return affectMhas;
    }

    @Override
    public Pair<Boolean,String> checkIsoMigrateStatus(List<String> mhas, String tag) throws SQLException {
        Set<String> mhaSet = Sets.newHashSet(mhas);
        ResourceTbl resourceTbl = new ResourceTbl();
        resourceTbl.setTag(tag);
        resourceTbl.setActive(BooleanEnum.TRUE.getCode());
        resourceTbl.setDeleted(BooleanEnum.FALSE.getCode());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryBy(resourceTbl);
        Set<String> ipsInTag = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toSet());
        
        Drc drc = metaProviderV2.getDrc();
        final AtomicBoolean isMatch = new AtomicBoolean(true);
        StringBuilder msg = new StringBuilder();
        for (Entry<String, Dc> dcEntry : drc.getDcs().entrySet()) {
            for (Entry<String, DbCluster> dbClusterEntry : dcEntry.getValue().getDbClusters().entrySet()) {
                DbCluster dbCluster = dbClusterEntry.getValue();
                if (mhaSet.contains(dbCluster.getMhaName())) {
                    // replicator
                    dbCluster.getReplicators().stream().map(Replicator::getIp).filter(ip -> !ipsInTag.contains(ip))
                            .forEach(ip -> {
                                isMatch.set(false);
                                msg.append("mha:").append(dbCluster.getMhaName()).append(" replicator not match:").append(ip).append("\n");
                            });
                     dbCluster.getAppliers().stream().map(Applier::getIp).filter(ip -> !ipsInTag.contains(ip))
                            .forEach(ip -> {
                                isMatch.set(false);
                                msg.append("mha:").append(dbCluster.getMhaName()).append(" applier not match:").append(ip).append("\n");
                            });
                    dbCluster.getMessengers().stream().map(Messenger::getIp).filter(ip -> !ipsInTag.contains(ip))
                            .forEach(ip -> {
                                isMatch.set(false);
                                msg.append("mha:").append(dbCluster.getMhaName()).append(" messenger not match:").append(ip).append("\n");
                            });
                }
            }
        }
        return Pair.of(isMatch.get(), msg.toString());
    }

    private void checkMhaTag(String tag, String mha, MhaTblV2 mhaTblV2) {
        if (!tag.equals(mhaTblV2.getTag())) {
            throw ConsoleExceptionUtils.message(mha + " mha tag not match");
        }
    }

    private void checkIsolationMigrateLimit(List<String> mhas) {
        if (mhas.size() > 10) {
            throw ConsoleExceptionUtils.message("mhas size must be less than 10");
        }
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

    private long initMhaAndAccount(MhaTblV2 mhaTblV2,List<MachineDto> machineDto) throws SQLException {
        MhaTblV2 existMhaTbl = mhaTblDao.queryByMhaName(mhaTblV2.getMhaName());
        if (null == existMhaTbl) {
            initAccountIfNeed(mhaTblV2,machineDto);
            return mhaTblDao.insertWithReturnId(mhaTblV2);
        } else if (existMhaTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
            initAccountIfNeed(existMhaTbl,machineDto);
            existMhaTbl.setMonitorSwitch(BooleanEnum.TRUE.getCode());
            existMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaTblDao.update(existMhaTbl);
        }
        return existMhaTbl.getId();
    }

    private void initAccountIfNeed(MhaTblV2 mhaTblV2,List<MachineDto> machineDto) throws SQLException {
        // account v2
        if (StringUtils.isBlank(mhaTblV2.getMonitorUserV2())
                || StringUtils.isBlank(mhaTblV2.getMonitorPasswordTokenV2())
                || StringUtils.isBlank(mhaTblV2.getReadUserV2())
                || StringUtils.isBlank(mhaTblV2.getReadPasswordTokenV2())
                || StringUtils.isBlank(mhaTblV2.getWriteUserV2())
                || StringUtils.isBlank(mhaTblV2.getWritePasswordTokenV2())) {
            // if no masterNode info , record default account v2
            if (CollectionUtils.isEmpty(machineDto)) {
                Account monitorAcc = kmsService.getAccountInfo(consoleConfig.getDefaultMonitorAccountKmsToken());
                Account readAcc = kmsService.getAccountInfo(consoleConfig.getDefaultReadAccountKmsToken());
                Account writeAcc = kmsService.getAccountInfo(consoleConfig.getDefaultWriteAccountKmsToken());
                mhaTblV2.setMonitorUserV2(monitorAcc.getUser());
                mhaTblV2.setMonitorPasswordTokenV2(accountService.encrypt(monitorAcc.getPassword()));
                mhaTblV2.setReadUserV2(readAcc.getUser());
                mhaTblV2.setReadPasswordTokenV2(accountService.encrypt(readAcc.getPassword()));
                mhaTblV2.setWriteUserV2(writeAcc.getUser());
                mhaTblV2.setWritePasswordTokenV2(accountService.encrypt(writeAcc.getPassword()));
            } else {
                if (checkAccountInitialized(mhaTblV2, machineDto)) {
                    return;
                }
                MachineDto masterNode = machineDto.stream().filter(MachineDto::getMaster).findFirst().orElse(null);
                if (masterNode == null) {
                    throw ConsoleExceptionUtils.message(mhaTblV2.getMhaName() + " masterNode not found!");
                }
                if (!accountService.mhaAccountV2ChangeAndRecord(mhaTblV2, masterNode.getIp(), masterNode.getPort())) {
                    throw ConsoleExceptionUtils.message(mhaTblV2.getMhaName() + " account change failed!");
                }
            }
        }
    }
    
    private boolean checkAccountInitialized(MhaTblV2 initMha, List<MachineDto> machineDtos) throws SQLException {
        for (MachineDto machineDto : machineDtos) {
            String ip = machineDto.getIp();
            Integer port = machineDto.getPort();
            MachineTbl existMachine = machineTblDao.queryByIpPort(ip, port);
            if (existMachine != null) {
                logger.warn("ambiguousMha:{},existMachine:{}",initMha.getMhaName(),existMachine);
                if (consoleConfig.getAllowAmbiguousMhaSwitch()) {
                    MhaTblV2 existMha = mhaTblDao.queryById(existMachine.getMhaId());
                    initMha.setMonitorUserV2(existMha.getMonitorUserV2());
                    initMha.setMonitorPasswordTokenV2(existMha.getMonitorPasswordTokenV2());
                    initMha.setReadUserV2(existMha.getReadUserV2());
                    initMha.setReadPasswordTokenV2(existMha.getReadPasswordTokenV2());
                    initMha.setWriteUserV2(existMha.getWriteUserV2());
                    initMha.setWritePasswordTokenV2(existMha.getWritePasswordTokenV2());
                    logger.warn("copy account from exist mha {}->{},", existMha.getMhaName(), initMha.getMhaName());
                } else {
                    throw ConsoleExceptionUtils.message("ambiguousMha:" + initMha.getMhaName() + ",existMachine:" + existMachine);
                }
                return true;
            }
        }
        return false;
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
    
    private void copyMhaProperties(MhaTblV2 copy,MhaTblV2 origin) {
        copy.setMonitorUserV2(origin.getMonitorUserV2());
        copy.setMonitorPasswordTokenV2(origin.getMonitorPasswordTokenV2());
        copy.setReadUserV2(origin.getReadUserV2());
        copy.setReadPasswordTokenV2(origin.getReadPasswordTokenV2());
        copy.setWriteUserV2(origin.getWriteUserV2());
        copy.setWritePasswordTokenV2(origin.getWritePasswordTokenV2());
        copy.setMonitorSwitch(origin.getMonitorSwitch());
        copy.setTag(origin.getTag());
        copy.setBuId(origin.getBuId());
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
