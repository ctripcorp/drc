package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.enums.log.LogBlackListType;
import com.ctrip.framework.drc.console.param.log.ConflictAutoHandleParam;
import com.ctrip.framework.drc.console.param.log.ConflictDbBlacklistQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.utils.*;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.core.monitor.util.ServicesUtil;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.user.IAMService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.sql.Timestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/9/26 16:06
 */
@Service
public class ConflictLogServiceImpl implements ConflictLogService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Autowired
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ColumnsFilterTblV2Dao columnsFilterTblV2Dao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MysqlServiceV2 mysqlService;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private ConflictDbBlackListTblDao conflictDbBlackListTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private DbBlacklistCache dbBlacklistCache;

    @Autowired
    private DbaApiService dbaApiService;

    private IAMService iamService = ServicesUtil.getIAMService();

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "conflictLog"));
    private final ListeningExecutorService cflExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newThreadExecutor(10, 50, 10000, "cflExecutorService"));

    private static final int BATCH_SIZE = 2000;
    private static final int SEVEN = 7;
    private static final int TWELVE = 12;
    private static final int Time_OUT = 60;
    private static final String ROW_LOG_ID = "drc_row_log_id";
    private static final String UPDATE_SQL = "UPDATE %s SET %s WHERE %s";
    private static final String INSERT_SQL = "INSERT INTO %s (%s) VALUES (%s)";
    private static final String DELETE_SQL = "DELETE FROM %s WHERE %s";
    private static final String EQUAL_SYMBOL = "=";
    private static final String MARKS = "`";
    private static final String EMPTY_SQL = "EMPTY_SQL";
    private static final String CELL_CLASS_TYPE = "cell-class-type";
    private static final String CELL_CLASS_NAME = "cellClassName";

    @Override
    public List<ConflictTrxLogView> getConflictTrxLogView(ConflictTrxLogQueryParam param) throws Exception {
        resetParam(param);

        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            return new ArrayList<>();
        }

        List<ConflictTrxLogView> views = conflictTrxLogTbls.stream().map(source -> {
            ConflictTrxLogView target = new ConflictTrxLogView();
            BeanUtils.copyProperties(source, target, "handleTime");
            target.setConflictTrxLogId(source.getId());
            target.setHandleTime(DateUtils.longToString(source.getHandleTime()));
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public List<ConflictRowsLogView> getConflictRowsLogView(ConflictRowsLogQueryParam param) throws Exception {
        resetParam(param);
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByParam(param);
        return getConflictRowsLogViews(conflictRowsLogTbls);
    }

    @Override
    public int getRowsLogCount(ConflictRowsLogQueryParam param) throws Exception {
        resetParam(param);
        return conflictRowsLogTblDao.getCount(param);
    }

    @Override
    public int getTrxLogCount(ConflictTrxLogQueryParam param) throws Exception {
        resetParam(param);
        return conflictTrxLogTblDao.getCount(param);
    }

    @Override
    public List<ConflictRowsLogView> getConflictRowsLogView(List<Long> conflictRowLogIds) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        return getConflictRowsLogViews(conflictRowsLogTbls);
    }

    @Override
    public ConflictTrxLogDetailView getConflictTrxLogDetailView(Long conflictTrxLogId) throws Exception {
        ConflictTrxLogDetailView view = new ConflictTrxLogDetailView();
        view.setConflictTrxLogId(conflictTrxLogId);

        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(conflictTrxLogId);
        if (conflictTrxLogTbl == null) {
            return view;
        }
        view.setTrxResult(conflictTrxLogTbl.getTrxResult());

        MhaTblV2 srcMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getSrcMhaName());
        MhaTblV2 dstMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getDstMhaName());
        DcTbl srcDcTbl = dcTblDao.queryById(srcMha.getDcId());
        DcTbl dstTbl = dcTblDao.queryById(dstMha.getDcId());
        view.setSrcRegion(srcDcTbl.getRegionName());
        view.setDstRegion(dstTbl.getRegionName());

        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }
        List<ConflictRowsLogDetailView> rowsLogDetailViews = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogDetailView target = new ConflictRowsLogDetailView();
            BeanUtils.copyProperties(source, target);
            target.setRowLogId(source.getId());
            return target;
        }).collect(Collectors.toList());
        view.setRowsLogDetailViews(rowsLogDetailViews);

        return view;
    }

    @Override
    public ConflictCurrentRecordView getConflictCurrentRecordView(Long conflictTrxLogId, int columnSize) throws Exception {
        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(conflictTrxLogId);
        if (conflictTrxLogTbl == null) {
            throw ConsoleExceptionUtils.message("trxLog not exist");
        }
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLog not exist");
        }

        String srcMhaName = conflictTrxLogTbl.getSrcMhaName();
        String dstMhaName = conflictTrxLogTbl.getDstMhaName();
        ConflictCurrentRecordView view = getConflictCurrentRecordView(conflictRowsLogTbls, srcMhaName, dstMhaName, columnSize);
        modifyRecords(view);
        return view;
    }

    @Override
    public ConflictCurrentRecordView getConflictRowRecordView(Long conflictRowLogId, int columnSize) throws Exception {
        ConflictRowsLogTbl rowLog = conflictRowsLogTblDao.queryById(conflictRowLogId);
        if (rowLog == null) {
            throw ConsoleExceptionUtils.message("rowLog not exist");
        }
        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(rowLog.getConflictTrxLogId());
        if (conflictTrxLogTbl == null) {
            throw ConsoleExceptionUtils.message("trxLog not exist");
        }
        if (StringUtils.isBlank(rowLog.getHandleSql()) && StringUtils.isBlank(rowLog.getRawSql())) {
            throw ConsoleExceptionUtils.message("conflict sql is empty");
        }
        String srcMhaName = conflictTrxLogTbl.getSrcMhaName();
        String dstMhaName = conflictTrxLogTbl.getDstMhaName();

        ConflictCurrentRecordView view = getConflictCurrentRecordView(Lists.newArrayList(rowLog), srcMhaName, dstMhaName, columnSize);
        modifyRecords(view);
        return view;
    }

    @Override
    public ConflictRowsRecordCompareView compareRowRecords(List<Long> conflictRowLogIds) throws Exception {
        ConflictRowsRecordCompareView view = new ConflictRowsRecordCompareView();

        List<ListenableFuture<Pair<Long, ConflictCurrentRecordView>>> futures = new ArrayList<>();
        for (long conflictRowLogId : conflictRowLogIds) {
            ListenableFuture<Pair<Long, ConflictCurrentRecordView>> future = executorService.submit(() -> getConflictRowRecordView(conflictRowLogId));
            futures.add(future);
        }

        Map<Long, ConflictCurrentRecordView> recordViewMap = new HashMap<>();
        for (ListenableFuture<Pair<Long, ConflictCurrentRecordView>> future : futures) {
            try {
                Pair<Long, ConflictCurrentRecordView> resultPair = future.get(5, TimeUnit.SECONDS);
                recordViewMap.put(resultPair.getLeft(), resultPair.getRight());
            } catch (Exception e) {
                logger.error("compare records error", e);
                throw ConsoleExceptionUtils.message(e.getMessage());
            }
        }

        List<Long> rowLogIds = new ArrayList<>();
        Map<String, Boolean> resultMap = new HashMap<>();
        recordViewMap.forEach((rowLogId, recordView) -> {
            Map<String, Object> srcRecord = recordView.getSrcRecords().get(0);
            boolean recordIsEqual = recordView.isRecordIsEqual();
            String tableName = (String) srcRecord.get("tableName");
            if (!resultMap.containsKey(tableName) || !recordIsEqual) {
                resultMap.put(tableName, recordIsEqual);
            }
            if (!recordIsEqual) {
                rowLogIds.add(rowLogId);
            }
        });

        List<ConflictRowsRecordDetail> recordDetailList = new ArrayList<>();
        resultMap.forEach((tableName, recordIsEqual) -> {
            ConflictRowsRecordDetail recordDetail = new ConflictRowsRecordDetail();
            recordDetail.setTableName(tableName);
            recordDetail.setRecordIsEqual(recordIsEqual);
            recordDetailList.add(recordDetail);
        });

        view.setRecordDetailList(recordDetailList);
        view.setRowLogIds(rowLogIds);
        return view;
    }

    private Pair<Long, ConflictCurrentRecordView> getConflictRowRecordView(long rowLogId) throws Exception {
        return Pair.of(rowLogId, getConflictRowRecordView(rowLogId, SEVEN));
    }

    @Override
    public void createConflictLog(List<ConflictTransactionLog> trxLogs) throws Exception {
        cflExecutorService.submit(() -> {
            try {
                insertConflictLog(trxLogs);
            } catch (Exception e) {
                throw ConsoleExceptionUtils.of(e);
            }
        });
    }

    public void insertConflictLog(List<ConflictTransactionLog> trxLogs) throws Exception {
        if (!consoleConfig.getConflictLogRecordSwitch()) {
            return;
        }
        trxLogs = filterTransactionLogs(trxLogs);
        if (CollectionUtils.isEmpty(trxLogs)) {
            return;
        }

        List<ConflictTrxLogTbl> conflictTrxLogTbls = trxLogs.stream().map(this::buildConflictTrxLog).collect(Collectors.toList());
        conflictTrxLogTbls = conflictTrxLogTblDao.batchInsertWithReturnId(conflictTrxLogTbls);
        Map<String, Long> trxLogMap = conflictTrxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getGtid, ConflictTrxLogTbl::getId));

        Set<String> mhaNames = new HashSet<>();
        for (ConflictTransactionLog trxLog : trxLogs) {
            mhaNames.add(trxLog.getSrcMha());
            mhaNames.add(trxLog.getDstMha());
        }
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(mhaNames), BooleanEnum.FALSE.getCode());
        Map<String, Long> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, MhaTblV2::getDcId));
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getRegionName));

        List<ConflictRowsLogTbl> conflictRowsLogTbls = new ArrayList<>();
        trxLogs.stream().forEach(trxLog -> {
            Long conflictTrxLogId = trxLogMap.get(trxLog.getGtid());
            final Integer isBrief = StringUtils.isEmpty(trxLog.getCflLogs().get(0).getRawSql()) ? 1 : 0;
            List<ConflictRowsLogTbl> conflictRowsLogList = buildConflictRowsLogs(conflictTrxLogId, trxLog, mhaMap, dcMap, isBrief);
            conflictRowsLogTbls.addAll(conflictRowsLogList);
        });
        conflictRowsLogTblDao.insert(conflictRowsLogTbls);
    }

    private void resetParam(ConflictTrxLogQueryParam param) {
        if (param.getBeginHandleTime() == null || param.getEndHandleTime() == null) {
            throw ConsoleExceptionUtils.message("beginTime or endTime requires not null");
        }
        if (param.getEndHandleTime() <= param.getBeginHandleTime()) {
            throw ConsoleExceptionUtils.message("endTime must be greater than beginTime");
        }
        param.setCreateBeginTime(DateUtils.getStartDateOfDay(param.getBeginHandleTime()));
        param.setCreateEndTime(DateUtils.getEndDateOfDay(param.getEndHandleTime() - 1));
        Pair<Boolean, List<String>> permissionAndDbsCanQuery = getPermissionAndDbsCanQuery();
        param.setAdmin(permissionAndDbsCanQuery.getLeft());
        param.setDbsWithPermission(permissionAndDbsCanQuery.getRight());
    }

    private void resetParam(ConflictRowsLogQueryParam param) throws Exception {
        if (param.getBeginHandleTime() == null || param.getEndHandleTime() == null) {
            throw ConsoleExceptionUtils.message("beginTime or endTime requires not null");
        }
        if (param.getEndHandleTime() <= param.getBeginHandleTime()) {
            throw ConsoleExceptionUtils.message("endTime must be greater than beginTime");
        }
        param.setCreateBeginTime(DateUtils.getStartDateOfDay(param.getBeginHandleTime()));
        param.setCreateEndTime(DateUtils.getEndDateOfDay(param.getEndHandleTime() - 1));

        Pair<Boolean, List<String>> adminAndDbs = getPermissionAndDbsCanQuery();
        param.setAdmin(adminAndDbs.getLeft());
        param.setDbsWithPermission(adminAndDbs.getRight());
        if (StringUtils.isNotBlank(param.getGtid())) {
            ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryByGtid(param.getGtid(), param.getBeginHandleTime(), param.getEndHandleTime());
            if (conflictTrxLogTbl != null) {
                param.setConflictTrxLogId(conflictTrxLogTbl.getId());
            }
        }
    }

    private List<ConflictTransactionLog> filterTransactionLogs(List<ConflictTransactionLog> trxLogs) throws Exception {
        trxLogs.stream().forEach(trxLog -> {
            List<ConflictRowLog> cflLogs = trxLog.getCflLogs().stream()
                    .filter(cflLog -> !isInBlackListWithCache(cflLog.getDb(), cflLog.getTable()))
                    .collect(Collectors.toList());
            trxLog.setCflLogs(cflLogs);
        });
        return trxLogs.stream().filter(trxLog -> !CollectionUtils.isEmpty(trxLog.getCflLogs())).collect(Collectors.toList());
    }

    @Override
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public long deleteTrxLogs(long beginTime, long endTime) throws Exception {
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByHandleTime(beginTime, endTime);
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            return 0;
        }
        List<Long> trxLogIds = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getId).collect(Collectors.toList());
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogIds(trxLogIds);

        batchDeleteTrxLogs(conflictTrxLogTbls);
        batchDeleteRowLogs(conflictRowsLogTbls);

        return conflictRowsLogTbls.size();
    }

    @Override
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public Map<String, Integer> deleteTrxLogsByTime(long beginTime, long endTime) throws Exception {
        Map<String, Integer> resultMap = new HashMap<>();
        int trxLogDeleteSize = conflictTrxLogTblDao.batchDeleteByHandleTime(beginTime, endTime);
        int rowLogDeleteSize = conflictRowsLogTblDao.batchDeleteByHandleTime(beginTime, endTime);

        resultMap.put("trxLogDeleteSize", trxLogDeleteSize);
        resultMap.put("rowLogDeleteSize", rowLogDeleteSize);
        return resultMap;
    }

    private Pair<Boolean, List<String>> getPermissionAndDbsCanQuery() {
        if (!iamService.canQueryAllCflLog().getLeft()) {
            List<String> dbsCanQuery = dbaApiService.getDBsWithQueryPermission();
            if (CollectionUtils.isEmpty(dbsCanQuery)) {
                throw ConsoleExceptionUtils.message("no db with DOT permission!");
            }
            return Pair.of(false, dbsCanQuery);
        } else {
            return Pair.of(true, Lists.newArrayList());
        }
    }

    @Override
    public Pair<String, String> checkSameMhaReplication(List<Long> conflictRowLogIds) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }

        return getMhaPair(conflictRowsLogTbls);
    }

    @Override
    public ConflictTrxLogDetailView getRowLogDetailView(List<Long> conflictRowLogIds) throws Exception {
        ConflictTrxLogDetailView view = new ConflictTrxLogDetailView();
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }

        List<ConflictRowsLogDetailView> detailViews = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogDetailView target = new ConflictRowsLogDetailView();
            BeanUtils.copyProperties(source, target);
            target.setRowLogId(source.getId());
            return target;
        }).collect(Collectors.toList());
        view.setRowsLogDetailViews(detailViews);

        Pair<String, String> mhaPair = getMhaPair(conflictRowsLogTbls);
        String srcMhaName = mhaPair.getLeft();
        String dstMhaName = mhaPair.getRight();
        MhaTblV2 srcMha = mhaTblV2Dao.queryByMhaName(srcMhaName);
        MhaTblV2 dstMha = mhaTblV2Dao.queryByMhaName(dstMhaName);
        DcTbl srcDcTbl = dcTblDao.queryById(srcMha.getDcId());
        DcTbl dstTbl = dcTblDao.queryById(dstMha.getDcId());
        view.setSrcRegion(srcDcTbl.getRegionName());
        view.setDstRegion(dstTbl.getRegionName());

        return view;
    }

    @Override
    public ConflictCurrentRecordView getConflictRowRecordView(List<Long> conflictRowLogIds) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }

        Pair<String, String> mhaPair = getMhaPair(conflictRowsLogTbls);
        String srcMhaName = mhaPair.getLeft();
        String dstMhaName = mhaPair.getRight();
        ConflictCurrentRecordView view = getConflictCurrentRecordView(conflictRowsLogTbls, srcMhaName, dstMhaName, TWELVE);
        modifyRecords(view);
        return view;
    }

    @Override
    public List<ConflictAutoHandleView> createHandleSql(ConflictAutoHandleParam param) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(param.getRowLogIds());
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }
        Pair<String, String> mhaPair = getMhaPair(conflictRowsLogTbls);
        String srcMhaName = mhaPair.getLeft();
        String dstMhaName = mhaPair.getRight();

        //query current records
        ConflictCurrentRecordView conflictRowRecordView = getConflictCurrentRecordView(conflictRowsLogTbls, srcMhaName, dstMhaName, TWELVE);

        Map<String, Object> srcResult = conflictRowRecordView.getSrcRecords().get(0);
        Map<String, Object> dstResult = conflictRowRecordView.getDstRecords().get(0);
        List<Map<String, Object>> srcRecords = (List<Map<String, Object>>) srcResult.get("records");
        List<Map<String, Object>> dstRecords = (List<Map<String, Object>>) dstResult.get("records");
        Map<String, String> srcColumnTypeMap = (Map<String, String>) srcResult.get("columnType");
        Map<String, String> dstColumnTypeMap = (Map<String, String>) dstResult.get("columnType");

        List<Map<String, Object>> srcColumnList = (List<Map<String, Object>>) srcResult.get("columns");
        List<Map<String, Object>> dstColumnList = (List<Map<String, Object>>) dstResult.get("columns");
        List<String> srcColumns = srcColumnList.stream().map(e -> String.valueOf(e.get("key"))).collect(Collectors.toList());
        List<String> dstColumns = dstColumnList.stream().map(e -> String.valueOf(e.get("key"))).collect(Collectors.toList());
        //get same columns
        srcColumns.retainAll(dstColumns);


        Map<String, List<String>> onUpdateColumnMap = getOnUpdateColumns(conflictRowsLogTbls, srcMhaName);
        Map<String, List<String>> uniqueIndexMap = getUniqueIndex(conflictRowsLogTbls, srcMhaName);

        Map<Long, Map<String, Object>> srcRecordMap = new HashMap<>();
        Map<Long, Map<String, Object>> dstRecordMap = new HashMap<>();
        for (Map<String, Object> srcRecord : srcRecords) {
            long rowLogId = Long.valueOf(String.valueOf(srcRecord.get(ROW_LOG_ID)));
            srcRecordMap.put(rowLogId, srcRecord);
        }
        for (Map<String, Object> dstRecord : dstRecords) {
            long rowLogId = Long.valueOf(String.valueOf(dstRecord.get(ROW_LOG_ID)));
            dstRecordMap.put(rowLogId, dstRecord);
        }

        boolean writeToDstMha = param.getWriteSide() == 0;
        Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair;
        if (writeToDstMha) {
            columnsFilerPair = getTableColumnsFilterFields(srcMhaName, dstMhaName);
        } else {
            columnsFilerPair = getTableColumnsFilterFields(dstMhaName, srcMhaName);
        }

        List<DbReplicationView> dbReplicationViews = columnsFilerPair.getLeft();
        Map<Long, List<String>> columnFieldMap = columnsFilerPair.getRight();
        List<ConflictAutoHandleView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictAutoHandleView target = new ConflictAutoHandleView();
            target.setRowLogId(source.getId());

            List<String> filerColumns = new ArrayList<>();
            if (!CollectionUtils.isEmpty(columnFieldMap)) {
                String tableName = MySqlUtils.toSqlField(source.getDbName()) + "." + MySqlUtils.toSqlField(source.getTableName());
                Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
                if (dbReplicationId != null && columnFieldMap.containsKey(dbReplicationId)) {
                    filerColumns = columnFieldMap.get(dbReplicationId);
                }
            }
            String handleSql = createConflictHandleSql(source, writeToDstMha, onUpdateColumnMap, uniqueIndexMap, Pair.of(filerColumns, srcColumns),
                    Pair.of(srcColumnTypeMap, dstColumnTypeMap), Pair.of(srcRecordMap, dstRecordMap));
            target.setAutoHandleSql(handleSql);
            return target;
        }).collect(Collectors.toList());
        return views.stream().filter(view -> StringUtils.isNotBlank(view.getAutoHandleSql())).collect(Collectors.toList());
    }

    @Override
    public void addDbBlacklist(String dbFilter, LogBlackListType type, Timestamp expirationTime) throws SQLException {
        List<ConflictDbBlackListTbl> tbls = conflictDbBlackListTblDao.queryBy(dbFilter, type.getCode());
        if (!CollectionUtils.isEmpty(tbls)) {
            logger.info("db blacklist already exist");
            return;
        }

        ConflictDbBlackListTbl tbl = new ConflictDbBlackListTbl();
        if (expirationTime == null) {
            int blacklistExpirationHour = domainConfig.getBlacklistExpirationHour(type);
            tbl.setExpirationTime(new Timestamp(System.currentTimeMillis() + (long) blacklistExpirationHour * 60 * 60 * 1000));
        } else {
            tbl.setExpirationTime(expirationTime);
        }
        tbl.setDbFilter(dbFilter);
        tbl.setType(type.getCode());
        conflictDbBlackListTblDao.insert(tbl);
        dbBlacklistCache.refresh();
    }

    @Override
    public void deleteBlacklist(String dbFilter) throws Exception {
        List<ConflictDbBlackListTbl> tbls = conflictDbBlackListTblDao.queryByDbFilter(dbFilter);
        if (CollectionUtils.isEmpty(tbls)) {
            logger.info("db blacklist not exist");
            return;
        }
        conflictDbBlackListTblDao.delete(tbls);
        dbBlacklistCache.refresh();
    }

    @Override
    public List<ConflictDbBlacklistView> getConflictDbBlacklistView(ConflictDbBlacklistQueryParam param) throws Exception {
        List<ConflictDbBlackListTbl> tbls = conflictDbBlackListTblDao.query(param);
        List<ConflictDbBlacklistView> views = tbls.stream().map(source -> {
            ConflictDbBlacklistView target = new ConflictDbBlacklistView();
            target.setId(source.getId());
            target.setDbFilter(source.getDbFilter());
            target.setType(source.getType());
            target.setCreateTime(DateUtils.longToString(source.getCreateTime().getTime()));

            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private void modifyRecords(ConflictCurrentRecordView view) {
        List<Map<String, Object>> srcRecords = view.getSrcRecords();
        List<Map<String, Object>> dstRecords = view.getDstRecords();

        srcRecords.forEach(srcRecord -> {
            List<Map<String, Object>> records = (List<Map<String, Object>>) srcRecord.get("records");
            if (!CollectionUtils.isEmpty(records)) {
                List<Map<String, Object>> extractRecords = records.stream().map(this::extractResult).collect(Collectors.toList());
                srcRecord.put("records", extractRecords);
            }
        });

        dstRecords.forEach(dstRecord -> {
            List<Map<String, Object>> records = (List<Map<String, Object>>) dstRecord.get("records");
            if (!CollectionUtils.isEmpty(records)) {
                List<Map<String, Object>> extractRecords = records.stream().map(this::extractResult).collect(Collectors.toList());
                dstRecord.put("records", extractRecords);
            }
        });
    }

    @Override
    public boolean isInBlackListWithCache(String db, String table) {
        String fullName = db + "." + table;
        List<AviatorRegexFilter> filters = dbBlacklistCache.getDbBlacklistInCache();
        for (AviatorRegexFilter filter : filters) {
            if (filter.filter(fullName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<ConflictRowRecordCompareEqualView> compareRowRecordsEqual(List<Long> conflictRowLogIds) throws Exception {
        if (CollectionUtils.isEmpty(conflictRowLogIds)) {
            throw ConsoleExceptionUtils.message("conflictRowLogIds are empty");
        }
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        Map<Long, List<ConflictRowsLogTbl>> rowsLogTblMap = conflictRowsLogTbls.stream().collect(Collectors.groupingBy(ConflictRowsLogTbl::getConflictTrxLogId));
        List<ConflictTrxLogTbl> trxLogTbls = conflictTrxLogTblDao.queryByIds(Lists.newArrayList(rowsLogTblMap.keySet()));
        Map<Long, ConflictTrxLogTbl> trxLogTblMap = trxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getId, Function.identity()));
        Map<MultiKey, List<ConflictTrxLogTbl>> multiTrxLogTblMap = trxLogTbls.stream().collect(Collectors.groupingBy(e -> new MultiKey(e.getSrcMhaName(), e.getDstMhaName())));


        List<ListenableFuture<ColumnsFilterAndIndexColumn>> futures = new ArrayList<>();

        for (Map.Entry<MultiKey, List<ConflictTrxLogTbl>> entry : multiTrxLogTblMap.entrySet()) {
            MultiKey multiKey = entry.getKey();
            List<ConflictTrxLogTbl> trxLogs = entry.getValue();

            List<ConflictRowsLogTbl> rowsLogTbls = new ArrayList<>();
            trxLogs.forEach(trxLog -> {
                rowsLogTbls.addAll(rowsLogTblMap.get(trxLog.getId()));
            });

            ListenableFuture<ColumnsFilterAndIndexColumn> future =
                    executorService.submit(() -> getColumnsFilterAndIndexColumn(multiKey, rowsLogTbls));
            futures.add(future);
        }

        Map<MultiKey, Pair<List<DbReplicationView>, Map<Long, List<String>>>> columnFilterMap = new HashMap<>();
        Map<String, List<String>> onUpdateColumnMap = new HashMap<>();
        Map<String, List<String>> uniqueIndexMap = new HashMap<>();
        for (ListenableFuture<ColumnsFilterAndIndexColumn> future : futures) {
            try {
                ColumnsFilterAndIndexColumn result = future.get(10, TimeUnit.SECONDS);
                columnFilterMap.put(result.getMultiKey(), result.getColumnsFilerPair());
                onUpdateColumnMap.putAll(result.getOnUpdateColumnMap());
                uniqueIndexMap.putAll(result.getUniqueIndexColumnMap());
            } catch (Exception e) {
                logger.error("compare row logs fail, {}", e);
                throw ConsoleExceptionUtils.message("compare log record fail");
            }
        }

        List<ListenableFuture<ConflictRowRecordCompareEqualView>> recordFutures = new ArrayList<>();
        for (ConflictRowsLogTbl rowLog : conflictRowsLogTbls) {
            String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
            List<String> onUpdateColumns = onUpdateColumnMap.getOrDefault(tableName, new ArrayList<>());
            List<String> uniqueIndexColumns = uniqueIndexMap.getOrDefault(tableName, new ArrayList<>());

            ConflictTrxLogTbl trxLogTbl = trxLogTblMap.get(rowLog.getConflictTrxLogId());
            String srcMhaName = trxLogTbl.getSrcMhaName();
            String dstMhaName = trxLogTbl.getDstMhaName();
            Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair = columnFilterMap.get(new MultiKey(srcMhaName, dstMhaName));


            ListenableFuture<ConflictRowRecordCompareEqualView> future = executorService.submit(() ->
                    getRowRecordCompareView(rowLog, srcMhaName, dstMhaName, Pair.of(onUpdateColumns, uniqueIndexColumns), columnsFilerPair.getLeft(), columnsFilerPair.getRight()));
            recordFutures.add(future);
        }

        List<ConflictRowRecordCompareEqualView> views = new ArrayList<>();
        for (ListenableFuture<ConflictRowRecordCompareEqualView> future : recordFutures) {
            try {
                ConflictRowRecordCompareEqualView view = future.get(10, TimeUnit.SECONDS);
                views.add(view);
            } catch (Exception e) {
                logger.error("compare row record error, {}", e);
            }
        }

        return views;
    }

    @Override
    public ConflictRowsLogCountView getRowsLogCountView(long beginHandleTime, long endHandlerTime) throws Exception {
        Future<List<ConflictRowsLogCount>> dbCountFuture = executorService.submit(() -> conflictRowsLogTblDao.queryTopNDb(beginHandleTime, endHandlerTime));
        Future<List<ConflictRowsLogCount>> rollBackDbCountsFuture = executorService.submit(() -> conflictRowsLogTblDao.queryTopNDb(beginHandleTime, endHandlerTime, BooleanEnum.TRUE.getCode()));
        Future<Integer> totalCountFuture = executorService.submit(() -> conflictRowsLogTblDao.queryCount(beginHandleTime, endHandlerTime));
        Future<Integer> rollBackCountFuture = executorService.submit(() -> conflictRowsLogTblDao.queryCount(beginHandleTime, endHandlerTime, BooleanEnum.TRUE.getCode()));

        ConflictRowsLogCountView view = new ConflictRowsLogCountView();
        Integer totalCount = null;
        try {
            totalCount = totalCountFuture.get(Time_OUT, TimeUnit.SECONDS);
            view.setTotalCount(totalCount);
        } catch (Exception e) {
            logger.warn("query totalCount timeout");
        }

        Integer rollBackCount = null;
        try {
            rollBackCount = rollBackCountFuture.get(Time_OUT, TimeUnit.SECONDS);
            view.setRollBackTotalCount(rollBackCount);
        } catch (Exception e) {
            logger.warn("query rollBackCount timeout");
        }

        List<ConflictRowsLogCount> dbCounts = null;
        try {
            dbCounts = dbCountFuture.get(Time_OUT, TimeUnit.SECONDS);
            view.setDbCounts(dbCounts);
        } catch (Exception e) {
            logger.warn("query dbCount timeout");
        }

        List<ConflictRowsLogCount> rollBackDbCounts = null;
        try {
            rollBackDbCounts = rollBackDbCountsFuture.get(Time_OUT, TimeUnit.SECONDS);
            view.setRollBackDbCounts(rollBackDbCounts);
        } catch (Exception e) {
            logger.warn("query rollBackDbCount timeout");
        }

        return view;
    }

    private ConflictRowRecordCompareEqualView getRowRecordCompareView(ConflictRowsLogTbl rowLog,
                                                                      String srcMhaName,
                                                                      String dstMhaName,
                                                                      Pair<List<String>, List<String>> indexColumnPair,
                                                                      List<DbReplicationView> dbReplicationViews,
                                                                      Map<Long, List<String>> columnsFieldMap) {
        ConflictRowRecordCompareEqualView view = new ConflictRowRecordCompareEqualView();
        view.setRowLogId(rowLog.getId());
        Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> record =
                queryRecords(rowLog, srcMhaName, dstMhaName, TWELVE, indexColumnPair, dbReplicationViews, columnsFieldMap);
        view.setRecordIsEqual(record.getLeft());
        return view;
    }

    private ColumnsFilterAndIndexColumn getColumnsFilterAndIndexColumn(MultiKey multiKey, List<ConflictRowsLogTbl> rowsLogTbls) throws Exception {
        String srcMhaName = (String) multiKey.getKey(0);
        String dstMhaName = (String) multiKey.getKey(1);
        Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair = getTableColumnsFilterFields(srcMhaName, dstMhaName);
        Map<String, List<String>> onUpdateColumnMap = getOnUpdateColumns(rowsLogTbls, srcMhaName);
        Map<String, List<String>> uniqueIndexColumnMap = getUniqueIndex(rowsLogTbls, srcMhaName);

        return new ColumnsFilterAndIndexColumn(multiKey, columnsFilerPair, onUpdateColumnMap, uniqueIndexColumnMap);
    }


    private String createConflictHandleSql(ConflictRowsLogTbl rowLog,
                                           boolean writeToDstMha,
                                           Map<String, List<String>> onUpdateColumnMap,
                                           Map<String, List<String>> uniqueIndexMap,
                                           Pair<List<String>, List<String>> columnPair,
                                           Pair<Map<String, String>, Map<String, String>> columnTypePair,
                                           Pair<Map<Long, Map<String, Object>>, Map<Long, Map<String, Object>>> recordPair) {
        String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
        List<String> onUpdateColumns = onUpdateColumnMap.get(tableName);
        String onUpdateColumn = null;
        if (!CollectionUtils.isEmpty(onUpdateColumns)) {
            onUpdateColumn = onUpdateColumns.get(0);
        }
        List<String> uniqueIndexColumns = uniqueIndexMap.get(tableName);

        Map<Long, Map<String, Object>> srcRecordMap = recordPair.getLeft();
        Map<Long, Map<String, Object>> dstRecordMap = recordPair.getRight();

        Map<String, Object> srcRecord = srcRecordMap.get(rowLog.getId());
        Map<String, Object> dstRecord = dstRecordMap.get(rowLog.getId());
        Map<String, Object> sourceRecord = writeToDstMha ? srcRecord : dstRecord;
        Map<String, Object> targetRecord = writeToDstMha ? dstRecord : srcRecord;

        Map<String, String> srcColumnType = columnTypePair.getLeft();
        Map<String, String> dstColumnType = columnTypePair.getRight();
        Map<String, String> sourceColumnType = writeToDstMha ? srcColumnType : dstColumnType;
        Map<String, String> targetColumnType = writeToDstMha ? dstColumnType : srcColumnType;

        return createConflictHandleSql(rowLog.getDbName(), rowLog.getTableName(), onUpdateColumn, uniqueIndexColumns, columnPair,
                Pair.of(sourceRecord, targetRecord), Pair.of(sourceColumnType, targetColumnType));
    }

    private String createConflictHandleSql(String dbName,
                                           String tableName,
                                           String onUpdateColumn,
                                           List<String> uniqueIndexColumns,
                                           Pair<List<String>, List<String>> columnPair,
                                           Pair<Map<String, Object>, Map<String, Object>> recordPair,
                                           Pair<Map<String, String>, Map<String, String>> columnTypePair) {
        Map<String, Object> sourceRecord = recordPair.getLeft();
        Map<String, Object> targetRecord = recordPair.getRight();
        Map<String, String> sourceColumnType = columnTypePair.getLeft();
        Map<String, String> targetColumnType = columnTypePair.getRight();
        List<String> filterColumns = columnPair.getLeft();
        List<String> commonColumns = columnPair.getRight();

        String fullTableName = MySqlUtils.toSqlField(dbName) + "." + MySqlUtils.toSqlField(tableName);
        if (sourceRecord != null && targetRecord != null) {  //update
            return createUpdateSql(filterColumns, fullTableName, onUpdateColumn, uniqueIndexColumns, recordPair, columnTypePair);
        } else if (sourceRecord != null) {  //insert
            return createInsertSql(fullTableName, filterColumns, commonColumns, sourceColumnType, sourceRecord);
        } else if (targetRecord != null) {  //delete
            return createDeleteSql(fullTableName, onUpdateColumn, uniqueIndexColumns, targetRecord, targetColumnType);
        } else {
            return EMPTY_SQL;
        }
    }

    //onUpdateColumn - `datachange_lasttime`, uniqueIndex - id
    private String createDeleteSql(String tableName, String onUpdateColumn, List<String> uniqueIndexColumns, Map<String, Object> targetRecord, Map<String, String> targetColumnType) {
        StringBuilder whereCondition = new StringBuilder();
        String uniqueIndexCondition = Joiner.on(" AND ").join(uniqueIndexColumns.stream().map(uniqueIndex -> buildUniqueIndexCondition(uniqueIndex, targetRecord, targetColumnType)).collect(Collectors.toList()));

        String onUpdateColumnName = onUpdateColumn.replace(MARKS, "");
        String onUpdateCondition = onUpdateColumn + EQUAL_SYMBOL + MySqlUtils.toSqlValue(targetRecord.get(onUpdateColumnName), targetColumnType.get(onUpdateColumnName));
        whereCondition.append(uniqueIndexCondition).append(" AND ").append(onUpdateCondition);

        return String.format(DELETE_SQL, tableName, whereCondition);
    }

    private String createInsertSql(String tableName, List<String> filterColumns, List<String> commonColumns, Map<String, String> sourceColumnType, Map<String, Object> sourceRecord) {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        sourceRecord.forEach((column, value) -> {
            if (!ROW_LOG_ID.equals(column) && !filterColumns.contains(column) && commonColumns.contains(column)) {
                columns.add(MySqlUtils.toSqlField(column));
                values.add(MySqlUtils.toSqlValue(value, sourceColumnType.get(column)));
            }
        });

        if (CollectionUtils.isEmpty(columns)) {
            return StringUtils.EMPTY;
        }
        String columnSql = Joiner.on(",").join(columns);
        String valueSql = Joiner.on(",").join(values);
        return String.format(INSERT_SQL, tableName, columnSql, valueSql);
    }

    private String createUpdateSql(List<String> filterColumns,
                                   String tableName,
                                   String onUpdateColumn,
                                   List<String> uniqueIndexColumns,
                                   Pair<Map<String, Object>, Map<String, Object>> recordPair,
                                   Pair<Map<String, String>, Map<String, String>> columnTypePair) {
        Map<String, Object> sourceRecord = recordPair.getLeft();
        Map<String, Object> targetRecord = recordPair.getRight();
        Map<String, String> sourceColumnType = columnTypePair.getLeft();
        Map<String, String> targetColumnType = columnTypePair.getRight();

        List<String> setFields = new ArrayList<>();
        Map<String, String> unEqualColumnMap = (Map<String, String>) sourceRecord.get(CELL_CLASS_NAME);
        if (CollectionUtils.isEmpty(unEqualColumnMap)) {
            return StringUtils.EMPTY;
        }
        Set<String> unEqualColumns = unEqualColumnMap.keySet();
        Set<String> targetColumns = targetRecord.keySet();

        sourceRecord.forEach((column, value) -> {
            if (unEqualColumns.contains(column) && !filterColumns.contains(column) && targetColumns.contains(column)) {
                String columnType = sourceColumnType.get(column);
                String setField = MySqlUtils.toSqlField(column) + EQUAL_SYMBOL + MySqlUtils.toSqlValue(value, columnType);
                setFields.add(setField);
            }
        });

        if (CollectionUtils.isEmpty(setFields)) {
            return StringUtils.EMPTY;
        }
        String setFiledSql = Joiner.on(",").join(setFields);

        StringBuilder whereCondition = new StringBuilder();
        String uniqueIndexCondition = Joiner.on(" AND ").join(uniqueIndexColumns.stream().map(uniqueIndex -> buildUniqueIndexCondition(uniqueIndex, sourceRecord, sourceColumnType)).collect(Collectors.toList()));

        String onUpdateColumnName = onUpdateColumn.replace(MARKS, "");
        String onUpdateCondition = onUpdateColumn + EQUAL_SYMBOL + MySqlUtils.toSqlValue(targetRecord.get(onUpdateColumnName), targetColumnType.get(onUpdateColumnName));
        whereCondition.append(uniqueIndexCondition).append(" AND ").append(onUpdateCondition);

        return String.format(UPDATE_SQL, tableName, setFiledSql, whereCondition);
    }

    private String buildUniqueIndexCondition(String uniqueIndex, Map<String, Object> record, Map<String, String> columnType) {
        return MySqlUtils.toSqlField(uniqueIndex) + EQUAL_SYMBOL + MySqlUtils.toSqlValue(record.get(uniqueIndex), columnType.get(uniqueIndex));
    }

    private Pair<String, String> getMhaPair(List<ConflictRowsLogTbl> conflictRowsLogTbls) throws SQLException {
        List<String> dbNames = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getDbName).distinct().collect(Collectors.toList());
        if (dbNames.size() != 1) {
            throw ConsoleExceptionUtils.message("can not select different db");
        }

        List<Long> trxLogIds = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getConflictTrxLogId).distinct().collect(Collectors.toList());
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByIds(trxLogIds);
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            throw ConsoleExceptionUtils.message("trxLogs not exist");
        }
        List<String> srcMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getSrcMhaName).distinct().collect(Collectors.toList());
        List<String> dstMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getDstMhaName).distinct().collect(Collectors.toList());

        if (srcMhaNames.size() != 1 || dstMhaNames.size() != 1) {
            throw ConsoleExceptionUtils.message("selected rowLogs belong to different mhaReplication");
        }
        return Pair.of(srcMhaNames.get(0), dstMhaNames.get(0));
    }

    private ConflictCurrentRecordView getConflictCurrentRecordView(List<ConflictRowsLogTbl> conflictRowsLogTbls, String srcMhaName, String dstMhaName, int columnSize) throws Exception {
        ConflictCurrentRecordView view = new ConflictCurrentRecordView();

        Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair = getTableColumnsFilterFields(srcMhaName, dstMhaName);
        Map<String, List<String>> onUpdateColumnMap = getOnUpdateColumns(conflictRowsLogTbls, srcMhaName);
        Map<String, List<String>> uniqueIndexMap = getUniqueIndex(conflictRowsLogTbls, srcMhaName);

        List<ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>>> futures = new ArrayList<>();
        for (ConflictRowsLogTbl rowLog : conflictRowsLogTbls) {
            String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
            List<String> onUpdateColumns = onUpdateColumnMap.getOrDefault(tableName, new ArrayList<>());
            List<String> uniqueIndexColumns = uniqueIndexMap.getOrDefault(tableName, new ArrayList<>());
            ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>> future = executorService.submit(() ->
                    queryRecords(rowLog, srcMhaName, dstMhaName, columnSize, Pair.of(onUpdateColumns, uniqueIndexColumns), columnsFilerPair.getLeft(), columnsFilerPair.getRight()));
            futures.add(future);
        }

        boolean recordIsEqual = true;
        Map<String, List<Map<String, Object>>> srcResultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> dstResultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> srcColumnMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> dstColumnMap = new HashMap<>();
        Map<String, Map<String, String>> srcColumnTypeMap = new HashMap<>();
        Map<String, Map<String, String>> dstColumnTypeMap = new HashMap<>();
        for (ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>> future : futures) {
            try {
                Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> resultPair = future.get(10, TimeUnit.SECONDS);
                recordIsEqual &= resultPair.getLeft();
                Map<String, Object> srcResult = resultPair.getRight().getLeft();
                Map<String, Object> dstResult = resultPair.getRight().getRight();

                extractRecords(srcResultMap, srcColumnMap, srcColumnTypeMap, srcResult);
                extractRecords(dstResultMap, dstColumnMap, dstColumnTypeMap, dstResult);
            } catch (Exception e) {
                logger.error("query records error: {}", e);
                throw ConsoleExceptionUtils.of(e);
            }
        }

        List<DbReplicationView> dbReplicationViews = drcBuildServiceV2.getDbReplicationView(dstMhaName, srcMhaName);
        List<Map<String, Object>> srcRecords = new ArrayList<>();
        List<Map<String, Object>> dstRecords = new ArrayList<>();
        srcResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = srcColumnMap.get(tableName);
            Map<String, String> columnType = srcColumnTypeMap.get(tableName);

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            resultMap.put("tableName", tableName);
            resultMap.put("columnType", columnType);

            Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
            boolean doubleSync = dbReplicationId != null;
            resultMap.put("doubleSync", doubleSync);
            srcRecords.add(resultMap);
        });
        dstResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = dstColumnMap.get(tableName);
            Map<String, String> columnType = dstColumnTypeMap.get(tableName);

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            resultMap.put("tableName", tableName);
            resultMap.put("columnType", columnType);

            Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
            boolean doubleSync = dbReplicationId != null;
            resultMap.put("doubleSync", doubleSync);
            dstRecords.add(resultMap);
        });
        view.setSrcRecords(srcRecords);
        view.setDstRecords(dstRecords);
        view.setRecordIsEqual(recordIsEqual);
        return view;
    }

    private Map<String, Object> extractResult(Map<String, Object> result) {
        if (CollectionUtils.isEmpty(result)) {
            return result;
        }

        Map<String, Object> extractResult = new HashMap<>();
        result.forEach((key, value) -> {
            if (value instanceof Long) {
                extractResult.put(key, String.valueOf(value));
            } else if (value instanceof byte[]) {
                extractResult.put(key, CommonUtils.byteToHexString((byte[]) value));
            } else if (value instanceof BigDecimal) {
                extractResult.put(key, String.valueOf(value));
            } else {
                extractResult.put(key, value);
            }
        });
        return extractResult;
    }

    private List<ConflictRowsLogView> getConflictRowsLogViews(List<ConflictRowsLogTbl> conflictRowsLogTbls) throws SQLException {
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return new ArrayList<>();
        }

        List<Long> conflictTrxLogIds = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getConflictTrxLogId).collect(Collectors.toList());
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByIds(conflictTrxLogIds);

        Map<Long, ConflictTrxLogTbl> conflictTrxLogMap = conflictTrxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getId, Function.identity()));

        List<ConflictRowsLogView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogView target = new ConflictRowsLogView();
            BeanUtils.copyProperties(source, target, "handleTime");
            target.setHandleTime(DateUtils.longToString(source.getHandleTime()));
            target.setConflictRowsLogId(source.getId());

            ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogMap.get(source.getConflictTrxLogId());
            target.setGtid(conflictTrxLogTbl.getGtid());
            target.setConflictTrxLogId(conflictTrxLogTbl.getId());
            target.setSrcRegion(source.getSrcRegion());
            target.setDstRegion(source.getDstRegion());
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private int batchDeleteTrxLogs(List<ConflictTrxLogTbl> conflictTrxLogTbls) throws Exception {
        int resultSize = 0;
        if (CollectionUtils.isEmpty(conflictTrxLogTbls)) {
            return resultSize;
        }
        int size = conflictTrxLogTbls.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_SIZE, size);
            List<ConflictTrxLogTbl> subTrxLogTbls = conflictTrxLogTbls.subList(i, toIndex);
            i += BATCH_SIZE;
            resultSize += conflictTrxLogTblDao.batchDelete(subTrxLogTbls).length;
            logger.info("batchDelete trxLogs size: " + resultSize);
        }
        return resultSize;
    }

    private int batchDeleteRowLogs(List<ConflictRowsLogTbl> conflictRowsLogTbls) throws Exception {
        int resultSize = 0;
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return resultSize;
        }
        int size = conflictRowsLogTbls.size();
        for (int i = 0; i < size; ) {
            int toIndex = Math.min(i + BATCH_SIZE, size);
            List<ConflictRowsLogTbl> subRowsLogTbls = conflictRowsLogTbls.subList(i, toIndex);
            i += BATCH_SIZE;
            resultSize += conflictRowsLogTblDao.batchDelete(subRowsLogTbls).length;
            logger.info("batchDelete rowLogs size: " + resultSize);
        }
        return resultSize;
    }

    private Map<String, List<String>> getOnUpdateColumns(List<ConflictRowsLogTbl> conflictRowsLogTbls, String mhaName) {
        List<String> tableNames = conflictRowsLogTbls.stream().map(rowLog -> rowLog.getDbName() + "." + rowLog.getTableName()).distinct().collect(Collectors.toList());
        List<ListenableFuture<Pair<String, List<String>>>> columnFutures = new ArrayList<>();
        for (String tableName : tableNames) {
            ListenableFuture<Pair<String, List<String>>> future = executorService.submit(() -> queryOnUpdateColumns(mhaName, tableName));
            columnFutures.add(future);
        }

        Map<String, List<String>> columnMap = new HashMap<>();
        for (ListenableFuture<Pair<String, List<String>>> future : columnFutures) {
            try {
                Pair<String, List<String>> resultPair = future.get(5, TimeUnit.SECONDS);
                columnMap.put(resultPair.getLeft(), resultPair.getRight());
            } catch (Exception e) {
                logger.error("queryOnUpdateColumns error mha: {}", mhaName, e);
                throw ConsoleExceptionUtils.message("queryOnUpdateColumns error");
            }
        }
        return columnMap;
    }

    private Map<String, List<String>> getUniqueIndex(List<ConflictRowsLogTbl> conflictRowsLogTbls, String mhaName) {
        List<String> tableNames = conflictRowsLogTbls.stream().map(rowLog -> rowLog.getDbName() + "." + rowLog.getTableName()).distinct().collect(Collectors.toList());
        List<ListenableFuture<Pair<String, List<String>>>> futures = new ArrayList<>();
        for (String tableName : tableNames) {
            ListenableFuture<Pair<String, List<String>>> future = executorService.submit(() -> queryUniqueIndex(mhaName, tableName));
            futures.add(future);
        }

        Map<String, List<String>> uniqueIndexMap = new HashMap<>();
        for (ListenableFuture<Pair<String, List<String>>> future : futures) {
            try {
                Pair<String, List<String>> resultPair = future.get(5, TimeUnit.SECONDS);
                uniqueIndexMap.put(resultPair.getLeft(), resultPair.getRight());
            } catch (Exception e) {
                logger.error("getUniqueIndex error mha: {}", mhaName, e);
                throw ConsoleExceptionUtils.message("getUniqueIndex error");
            }
        }
        return uniqueIndexMap;
    }

    private Pair<String, List<String>> queryOnUpdateColumns(String mha, String tableName) {
        String[] tables = tableName.split("\\.");
        List<String> onUpdateColumns = mysqlService.getAllOnUpdateColumns(mha, tables[0], tables[1]);
        onUpdateColumns = onUpdateColumns.stream().map(column -> "`" + column + "`").collect(Collectors.toList());
        return Pair.of(tableName, onUpdateColumns);
    }

    private Pair<String, List<String>> queryUniqueIndex(String mha, String tableName) {
        String[] tables = tableName.split("\\.");
        List<String> indexColumns = mysqlService.getUniqueIndex(mha, tables[0], tables[1]);
        return Pair.of(tableName, indexColumns);
    }

    private ConflictTrxLogTbl buildConflictTrxLog(ConflictTransactionLog trxLog) {
        ConflictTrxLogTbl conflictTrxLogTbl = new ConflictTrxLogTbl();
        conflictTrxLogTbl.setSrcMhaName(trxLog.getSrcMha());
        conflictTrxLogTbl.setDstMhaName(trxLog.getDstMha());
        conflictTrxLogTbl.setGtid(trxLog.getGtid());
        conflictTrxLogTbl.setTrxRowsNum(trxLog.getTrxRowsNum());
        conflictTrxLogTbl.setCflRowsNum(trxLog.getCflRowsNum());
        conflictTrxLogTbl.setTrxResult(trxLog.getTrxRes());
        conflictTrxLogTbl.setHandleTime(trxLog.getHandleTime());
        conflictTrxLogTbl.setDb(trxLog.getCflLogs().get(0).getDb());
        return conflictTrxLogTbl;
    }

    private List<ConflictRowsLogTbl> buildConflictRowsLogs(long conflictTrxLogId, ConflictTransactionLog trxLog, Map<String, Long> mhaMap, Map<Long, String> dcMap, Integer isBrief) {
        long srcDcId = mhaMap.getOrDefault(trxLog.getSrcMha(), 0L);
        long dstDcId = mhaMap.getOrDefault(trxLog.getDstMha(), 0L);
        String srcRegion = dcMap.getOrDefault(srcDcId, "");
        String dstRegion = dcMap.getOrDefault(dstDcId, "");

        List<ConflictRowsLogTbl> conflictTrxLogTbls = trxLog.getCflLogs().stream().map(source -> {
            ConflictRowsLogTbl target = new ConflictRowsLogTbl();
            target.setConflictTrxLogId(conflictTrxLogId);
            target.setDbName(source.getDb());
            target.setTableName(source.getTable());
            target.setRawSql(source.getRawSql());
            target.setRawSqlResult(source.getRawRes());
            target.setHandleSql(source.getHandleSql());
            target.setHandleSqlResult(source.getHandleSqlRes());
            target.setDstRowRecord(source.getDstRecord());
            target.setRowResult(source.getRowRes());
            target.setHandleTime(trxLog.getHandleTime());
            target.setRowId(source.getRowId());
            target.setSrcRegion(srcRegion);
            target.setDstRegion(dstRegion);
            target.setBrief(isBrief);
            return target;
        }).collect(Collectors.toList());

        return conflictTrxLogTbls;
    }

    private void extractRecords(Map<String, List<Map<String, Object>>> resultMap,
                                Map<String, List<Map<String, Object>>> columnMap,
                                Map<String, Map<String, String>> srcColumnTypeMap,
                                Map<String, Object> result) {
        if (CollectionUtils.isEmpty(result)) {
            return;
        }
        String tableName = String.valueOf(result.get("tableName"));
        List<Map<String, Object>> recordList = (List<Map<String, Object>>) result.get("record");
        if (resultMap.containsKey(tableName)) {
            resultMap.get(tableName).addAll(recordList);
        } else {
            resultMap.put(tableName, recordList);
            List<Map<String, Object>> columns = (List<Map<String, Object>>) result.get("metaColumn");
            columnMap.put(tableName, columns);
            srcColumnTypeMap.put(tableName, (Map<String, String>) result.get("columnType"));
        }
    }

    private Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> queryRecords(ConflictRowsLogTbl rowLog,
                                                                                       String srcMhaName,
                                                                                       String dstMhaName,
                                                                                       int columnSize,
                                                                                       Pair<List<String>, List<String>> indexColumnPair,
                                                                                       List<DbReplicationView> dbReplicationViews,
                                                                                       Map<Long, List<String>> columnsFieldMap) {
        String sql = StringUtils.isNotBlank(rowLog.getHandleSql()) ? rowLog.getHandleSql() : rowLog.getRawSql();

        if (StringUtils.isEmpty(sql)) {
            throw ConsoleExceptionUtils.message("conflict sql is empty");
        }

        Map<String, Object> srcResultMap;
        Map<String, Object> dstResultMap;
        List<String> onUpdateColumns = indexColumnPair.getLeft();
        List<String> uniqueIndexColumn = indexColumnPair.getRight().stream().map(MySqlUtils::toSqlField).collect(Collectors.toList());
        try {
            srcResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(srcMhaName, sql, onUpdateColumns, uniqueIndexColumn, columnSize));
            dstResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(dstMhaName, sql, onUpdateColumns, uniqueIndexColumn, columnSize));
        } catch (Exception e) {
            throw ConsoleExceptionUtils.message(e.getMessage());
        }
        boolean sameRecord = recordIsEqual(srcResultMap, dstResultMap, columnsFieldMap, dbReplicationViews);

        List<Map<String, Object>> srcRecords = (List<Map<String, Object>>) srcResultMap.get("record");
        List<Map<String, Object>> dstRecords = (List<Map<String, Object>>) dstResultMap.get("record");
        if (!CollectionUtils.isEmpty(srcRecords)) {
            srcRecords.get(0).put(ROW_LOG_ID, String.valueOf(rowLog.getId()));
        }
        if (!CollectionUtils.isEmpty(dstRecords)) {
            dstRecords.get(0).put(ROW_LOG_ID, String.valueOf(rowLog.getId()));
        }
        return Pair.of(sameRecord, Pair.of(srcResultMap, dstResultMap));
    }

    private boolean recordIsEqual(Map<String, Object> srcResultMap,
                                  Map<String, Object> dstResultMap,
                                  Map<Long, List<String>> columnsFieldMap,
                                  List<DbReplicationView> dbReplicationViews) {
        List<Map<String, Object>> srcRecords = (List<Map<String, Object>>) srcResultMap.get("record");
        List<Map<String, Object>> dstRecords = (List<Map<String, Object>>) dstResultMap.get("record");

        if (CollectionUtils.isEmpty(srcRecords) && CollectionUtils.isEmpty(dstRecords)) {
            logger.info("both records are empty");
            return true;
        }
        if (CollectionUtils.isEmpty(srcRecords) || CollectionUtils.isEmpty(dstRecords)) {
            logger.info("src or dst records are empty");
            return false;
        }
        // `db`.`table`
        String tableName = (String) srcResultMap.get("tableName");
        List<String> columns = (List<String>) srcResultMap.get("columns");
        List<String> dstColumns = (List<String>) dstResultMap.get("columns");
        for (String column : dstColumns) {
            if (!columns.contains(column)) {
                columns.add(column);
            }
        }
        if (columnsFieldMap != null) {
            Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
            if (dbReplicationId != null) {
                List<String> filterColumns = columnsFieldMap.get(dbReplicationId);
                if (!CollectionUtils.isEmpty(filterColumns)) {
                    columns = columns.stream().filter(e -> !filterColumns.contains(e.toLowerCase())).collect(Collectors.toList());
                    List<Map<String, Object>> srcMetaColumns = (List<Map<String, Object>>) srcResultMap.get("metaColumn");
                    List<Map<String, Object>> dstMetaColumns = (List<Map<String, Object>>) dstResultMap.get("metaColumn");
                    setFilterColumnTip(srcMetaColumns, filterColumns);
                    setFilterColumnTip(dstMetaColumns, filterColumns);
                }
            }
        }

        Map<String, Object> srcRecord = srcRecords.get(0);
        Map<String, Object> dstRecord = dstRecords.get(0);

        Map<String, String> columnTypeMap = (Map<String, String>) srcResultMap.get("columnType");
        return recordIsEqual(columns, columnTypeMap, srcRecord, dstRecord);
    }

    private void setFilterColumnTip(List<Map<String, Object>> metaColumns, List<String> filterColumns) {
        for (Map<String, Object> metaColumn : metaColumns) {
            String column = (String) metaColumn.get("key");
            if (filterColumns.contains(column)) {
                metaColumn.put("title", column + " (字段过滤)");
            }
        }
    }

    private boolean recordIsEqual(List<String> columns, Map<String, String> columnTypeMap, Map<String, Object> srcRecord, Map<String, Object> dstRecord) {
        List<String> unEqualColumns = new ArrayList<>();
        for (String column : columns) {
            Object srcValue = srcRecord.get(column);
            Object dstValue = dstRecord.get(column);
            String columnType = columnTypeMap.get(column);
            if (srcValue == null && dstValue == null) {
                continue;
            }
            if (srcValue == null || dstValue == null) {
                unEqualColumns.add(column);
            } else if (!equal(srcValue, dstValue, columnType)) {
                unEqualColumns.add(column);
                logger.info("value not equal, column: {}, srcValue: {}, dstValue: {}", column, srcValue, dstValue);
            }
        }

        if (!CollectionUtils.isEmpty(unEqualColumns)) {
            Map<String, String> cellClassMap = new HashMap<>();
            for (String column : unEqualColumns) {
                cellClassMap.put(column, CELL_CLASS_TYPE);
            }
            srcRecord.put(CELL_CLASS_NAME, cellClassMap);
            dstRecord.put(CELL_CLASS_NAME, cellClassMap);
            return false;
        }
        return true;
    }

    private boolean equal(Object srcValue, Object dstValue, String columnType) {
        if (BigDecimal.class.getName().equals(columnType)) {
            return new BigDecimal(String.valueOf(srcValue)).compareTo(new BigDecimal(String.valueOf(dstValue))) == 0;
        }
        return String.valueOf(srcValue).equals(String.valueOf(dstValue));
    }

    private Long getDbReplicationIdByTableName(String tableName, List<DbReplicationView> dbReplicationViews) {
        String fullTableName = tableName.replace("`", "");
        String[] tables = fullTableName.split("\\.");
        String dbName = tables[0];
        String table = tables[1].toLowerCase();
        for (DbReplicationView view : dbReplicationViews) {
            if (!view.getDbName().equalsIgnoreCase(dbName)) {
                continue;
            }
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(view.getLogicTableName());
            if (aviatorRegexFilter.filter(table)) {
                return view.getDbReplicationId();
            }
        }
        return null;
    }

    private Pair<List<DbReplicationView>, Map<Long, List<String>>> getTableColumnsFilterFields(String srcMhaName, String dstMhaName) throws Exception {
        List<DbReplicationView> dbReplicationViews = drcBuildServiceV2.getDbReplicationView(srcMhaName, dstMhaName);
        List<DbReplicationView> columnsFilterDbReplications = dbReplicationViews.stream()
                .filter(e -> !CollectionUtils.isEmpty(e.getFilterTypes())
                        && e.getFilterTypes().contains(FilterTypeEnum.COLUMNS_FILTER.getCode()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(columnsFilterDbReplications)) {
            return Pair.of(dbReplicationViews, null);
        }

        List<Long> columnsFilterDbReplicationIds = columnsFilterDbReplications.stream().map(DbReplicationView::getDbReplicationId).collect(Collectors.toList());
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(columnsFilterDbReplicationIds);
        Set<Long> columnsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).filter(e -> e != -1L).collect(Collectors.toSet());

        List<ColumnsFilterTblV2> columnsFilterTblV2s = columnsFilterTblV2Dao.queryByIds(Lists.newArrayList(columnsFilterIds));
        Map<Long, List<String>> columnsFilterMap = columnsFilterTblV2s.stream().collect(
                Collectors.toMap(ColumnsFilterTblV2::getId, e -> JsonUtils.fromJsonToList(e.getColumns(), String.class)));
        Map<Long, Long> dbReplicationFilterMappingMap = dbReplicationFilterMappingTbls.stream().collect(
                Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, DbReplicationFilterMappingTbl::getColumnsFilterId));

        Map<Long, List<String>> columnsFieldMap = new HashMap<>();
        for (DbReplicationView dbReplicationView : columnsFilterDbReplications) {
            long dbReplicationId = dbReplicationView.getDbReplicationId();
            List<String> columnsFields = columnsFilterMap.get(dbReplicationFilterMappingMap.getOrDefault(dbReplicationId, -1L));
            if (!CollectionUtils.isEmpty(columnsFields)) {
                columnsFields = columnsFields.stream().map(String::toLowerCase).collect(Collectors.toList());
            }
            columnsFieldMap.put(dbReplicationId, columnsFields);
        }
        return Pair.of(dbReplicationViews, columnsFieldMap);
    }

    @Override
    public List<AviatorRegexFilter> queryBlackList() {
        try {
            List<AviatorRegexFilter> blackList = new ArrayList<>();
            List<ConflictDbBlackListTbl> blackListTbls = conflictDbBlackListTblDao.queryAllExist();
            for (ConflictDbBlackListTbl blackListTbl : blackListTbls) {
                blackList.add(new AviatorRegexFilter(blackListTbl.getDbFilter()));
            }
            return blackList;
        } catch (Exception e) {
            logger.error("queryBlackList error", e);
            return Collections.emptyList();
        }
    }

    private class ColumnsFilterAndIndexColumn {
        private MultiKey multiKey;
        private Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair;
        private Map<String, List<String>> onUpdateColumnMap;
        Map<String, List<String>> uniqueIndexColumnMap;


        public ColumnsFilterAndIndexColumn() {
        }

        public ColumnsFilterAndIndexColumn(MultiKey multiKey, Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair, Map<String, List<String>> onUpdateColumnMap, Map<String, List<String>> uniqueIndexColumnMap) {
            this.multiKey = multiKey;
            this.columnsFilerPair = columnsFilerPair;
            this.onUpdateColumnMap = onUpdateColumnMap;
            this.uniqueIndexColumnMap = uniqueIndexColumnMap;
        }

        public MultiKey getMultiKey() {
            return multiKey;
        }

        public void setMultiKey(MultiKey multiKey) {
            this.multiKey = multiKey;
        }

        public Pair<List<DbReplicationView>, Map<Long, List<String>>> getColumnsFilerPair() {
            return columnsFilerPair;
        }

        public void setColumnsFilerPair(Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair) {
            this.columnsFilerPair = columnsFilerPair;
        }

        public Map<String, List<String>> getOnUpdateColumnMap() {
            return onUpdateColumnMap;
        }

        public void setOnUpdateColumnMap(Map<String, List<String>> onUpdateColumnMap) {
            this.onUpdateColumnMap = onUpdateColumnMap;
        }

        public Map<String, List<String>> getUniqueIndexColumnMap() {
            return uniqueIndexColumnMap;
        }

        public void setUniqueIndexColumnMap(Map<String, List<String>> uniqueIndexColumnMap) {
            this.uniqueIndexColumnMap = uniqueIndexColumnMap;
        }
    }
}
