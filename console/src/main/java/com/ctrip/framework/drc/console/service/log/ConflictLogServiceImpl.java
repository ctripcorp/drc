package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.param.log.ConflictAutoHandleParam;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
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
    private DefaultConsoleConfig defaultConsoleConfig;

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(5, "conflictLog"));
    private final ListeningExecutorService compareExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(10, "conflictRowCompare"));
    private static final int BATCH_SIZE = 2000;
    private static final int SEVEN = 7;
    private static final int TWELVE = 12;
    private static final String ROW_LOG_ID = "drc_row_log_id";
    private static final String UPDATE_SQL = "UPDATE %s SET %s WHERE %s";
    private static final String INSERT_SQL = "INSERT INTO %s (%s) VALUES (%s)";
    private static final String DELETE_SQL = "DELETE FROM %s WHERE %s";
    private static final String EQUAL_SYMBOL = "=";
    private static final String MARKS = "`";
    private static final String EMPTY_SQL = "EMPTY_SQL";

    @Override
    public List<ConflictTrxLogView> getConflictTrxLogView(ConflictTrxLogQueryParam param) throws Exception {
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
        if (StringUtils.isNotBlank(param.getGtid())) {
            ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryByGtid(param.getGtid());
            if (conflictTrxLogTbl != null) {
                param.setConflictTrxLogId(conflictTrxLogTbl.getId());
            }
        }

        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByParam(param);
        return getConflictRowsLogViews(conflictRowsLogTbls);
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
        return getConflictCurrentRecordView(conflictRowsLogTbls, srcMhaName, dstMhaName, columnSize);
    }

    @Override
    public ConflictCurrentRecordView getConflictRowRecordView(Long conflictRowLogId, int columnSize) throws Exception {
        ConflictCurrentRecordView view = new ConflictCurrentRecordView();
        ConflictRowsLogTbl rowLog = conflictRowsLogTblDao.queryById(conflictRowLogId);
        if (rowLog == null) {
            throw ConsoleExceptionUtils.message("rowLog not exist");
        }
        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(rowLog.getConflictTrxLogId());
        if (conflictTrxLogTbl == null) {
            throw ConsoleExceptionUtils.message("trxLog not exist");
        }
        String srcMhaName = conflictTrxLogTbl.getSrcMhaName();
        String dstMhaName = conflictTrxLogTbl.getDstMhaName();
        String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
        Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair = getTableColumnsFilterFields(srcMhaName, dstMhaName);

        List<DbReplicationView> dbReplicationViews = drcBuildServiceV2.getDbReplicationView(dstMhaName, srcMhaName);
        Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
        boolean doubleSync = dbReplicationId != null;

        Pair<String, List<String>> onUpdateColumnPair = queryOnUpdateColumns(srcMhaName, tableName);
        List<String> onUpdateColumns = onUpdateColumnPair.getRight();

        Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> resultPair =
                queryRecords(rowLog, srcMhaName, dstMhaName, columnSize, onUpdateColumns, columnsFilerPair.getLeft(), columnsFilerPair.getRight());
        view.setRecordIsEqual(resultPair.getLeft());
        Map<String, Object> srcResult = resultPair.getRight().getLeft();
        Map<String, Object> dstResult = resultPair.getRight().getRight();

        Map<String, Object> srcRecord = new HashMap<>();
        Map<String, Object> dstRecord = new HashMap<>();
        srcRecord.put("columns", srcResult.get("metaColumn"));
        srcRecord.put("records", srcResult.get("record"));
        srcRecord.put("tableName", tableName);
        srcRecord.put("doubleSync", doubleSync);

        dstRecord.put("columns", dstResult.get("metaColumn"));
        dstRecord.put("records", dstResult.get("record"));
        dstRecord.put("tableName", tableName);
        dstRecord.put("doubleSync", doubleSync);

        view.setSrcRecords(Lists.newArrayList(srcRecord));
        view.setDstRecords(Lists.newArrayList(dstRecord));
        return view;
    }

    @Override
    public ConflictRowsRecordCompareView compareRowRecords(List<Long> conflictRowLogIds) throws Exception {
        ConflictRowsRecordCompareView view = new ConflictRowsRecordCompareView();

        List<ListenableFuture<Pair<Long, ConflictCurrentRecordView>>> futures = new ArrayList<>();
        for (long conflictRowLogId : conflictRowLogIds) {
            ListenableFuture<Pair<Long, ConflictCurrentRecordView>> future = compareExecutorService.submit(() -> getConflictRowRecordView(conflictRowLogId));
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
        trxLogs = filterTransactionLogs(trxLogs);
        if (CollectionUtils.isEmpty(trxLogs)) {
            logger.info("trxLogs are empty");
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
            List<ConflictRowsLogTbl> conflictRowsLogList = buildConflictRowsLogs(conflictTrxLogId, trxLog, mhaMap, dcMap);
            conflictRowsLogTbls.addAll(conflictRowsLogList);
        });
        conflictRowsLogTblDao.insert(conflictRowsLogTbls);
    }

    private List<ConflictTransactionLog> filterTransactionLogs(List<ConflictTransactionLog> trxLogs) {
        List<String> conflictDbBlacklist = defaultConsoleConfig.getConflictDbBlacklist();
        if (CollectionUtils.isEmpty(conflictDbBlacklist)) {
            return trxLogs;
        }

        String dbFilter = Joiner.on("|").join(conflictDbBlacklist);
        AviatorRegexFilter regexFilter = new AviatorRegexFilter(dbFilter);
        trxLogs.stream().forEach(trxLog -> {
            List<ConflictRowLog> cflLogs = trxLog.getCflLogs().stream().filter(cflLog -> {
                String tableName = cflLog.getDb() + "." + cflLog.getTable();
                return !regexFilter.filter(tableName);
            }).collect(Collectors.toList());
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

    @Override
    public Pair<String, String> checkSameMhaReplication(List<Long> conflictRowLogIds) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }

        return getMhaPair(conflictRowsLogTbls);
    }

    @Override
    public List<ConflictRowsLogDetailView> getConflictRowLogDetailView(List<Long> conflictRowLogIds) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(conflictRowLogIds);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return new ArrayList<>();
        }

        List<ConflictRowsLogDetailView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogDetailView target = new ConflictRowsLogDetailView();
            BeanUtils.copyProperties(source, target);
            target.setRowLogId(source.getId());
            return target;
        }).collect(Collectors.toList());
        return views;
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
        return getConflictCurrentRecordView(conflictRowsLogTbls, srcMhaName, dstMhaName, TWELVE);
    }

    @Override
    public List<ConflictAutoHandleView> createHandleSql(ConflictAutoHandleParam param) throws Exception {
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByIds(param.getRowLogIds());
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            throw ConsoleExceptionUtils.message("rowLogs not exist");
        }
        Pair<String, String> mhaPair = getMhaPair(conflictRowsLogTbls);
        String srcMhaName = mhaPair.getLeft();

        Map<String, List<String>> onUpdateColumnMap = getOnUpdateColumns(conflictRowsLogTbls, srcMhaName);
        Map<String, String> uniqueIndexMap = getUniqueIndex(conflictRowsLogTbls, srcMhaName);

        Map<Long, Map<String, Object>> srcRecordMap = new HashMap<>();
        Map<Long, Map<String, Object>> dstRecordMap = new HashMap<>();
        for (Map<String, Object> srcRecord : param.getSrcRecords()) {
            long rowLogId = Long.valueOf(String.valueOf(srcRecord.get(ROW_LOG_ID)));
            srcRecordMap.put(rowLogId, srcRecord);
        }
        for (Map<String, Object> dstRecord : param.getDstRecords()) {
            long rowLogId = Long.valueOf(String.valueOf(dstRecord.get(ROW_LOG_ID)));
            dstRecordMap.put(rowLogId, dstRecord);
        }

        boolean writeToDstMha = param.getWriteSide() == 0;
        List<ConflictAutoHandleView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictAutoHandleView target = new ConflictAutoHandleView();
            target.setRowLogId(source.getId());

            String handleSql = createConflictHandleSql(source, onUpdateColumnMap, uniqueIndexMap, srcRecordMap, dstRecordMap, writeToDstMha);
            target.setAutoHandleSql(handleSql);
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    private String createConflictHandleSql(ConflictRowsLogTbl rowLog,
                                           Map<String, List<String>> onUpdateColumnMap,
                                           Map<String, String> uniqueIndexMap,
                                           Map<Long, Map<String, Object>> srcRecordMap,
                                           Map<Long, Map<String, Object>> dstRecordMap,
                                           boolean writeToDstMha) {
        Map<String, Object> srcRecord = srcRecordMap.get(rowLog.getId());
        Map<String, Object> dstRecord = dstRecordMap.get(rowLog.getId());
        String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
        List<String> onUpdateColumns = onUpdateColumnMap.get(tableName);
        String onUpdateColumn = null;
        if (!CollectionUtils.isEmpty(onUpdateColumns)) {
            onUpdateColumn = onUpdateColumns.get(0);
        }
        String uniqueIndex = uniqueIndexMap.get(tableName);

        Map<String, Object> sourceRecord = writeToDstMha ? srcRecord : dstRecord;
        Map<String, Object> targetRecord = writeToDstMha ? dstRecord : srcRecord;
        return createConflictHandleSql(tableName, sourceRecord, targetRecord, onUpdateColumn, uniqueIndex);
    }

    private String createConflictHandleSql(String tableName, Map<String, Object> sourceRecord, Map<String, Object> targetRecord, String onUpdateColumn, String uniqueIndex) {
        if (sourceRecord != null && targetRecord != null) {  //update
            return createUpdateSql(sourceRecord, targetRecord,tableName,  onUpdateColumn, uniqueIndex);
        } else if (sourceRecord != null) {  //insert
            return createInsertSql(tableName, sourceRecord);
        } else if (targetRecord != null){  //delete
            return createDeleteSql(tableName, targetRecord, onUpdateColumn, uniqueIndex);
        } else {
            return EMPTY_SQL;
        }
    }

    //onUpdateColumn - `datachange_lasttime`, uniqueIndex - id
    private String createDeleteSql(String tableName, Map<String, Object> targetRecord, String onUpdateColumn, String uniqueIndex) {
        StringBuilder whereCondition = new StringBuilder();
        String uniqueIndexCondition = MySqlUtils.toSqlField(uniqueIndex) + EQUAL_SYMBOL + MySqlUtils.toSqlValue(targetRecord.get(uniqueIndex));
        String onUpdateCondition = onUpdateColumn + EQUAL_SYMBOL + MySqlUtils.toSqlValue(targetRecord.get(onUpdateColumn.replace(MARKS, "")));
        whereCondition.append(uniqueIndexCondition).append(" AND ").append(onUpdateCondition);

        return String.format(DELETE_SQL, MySqlUtils.toSqlField(tableName), whereCondition);
    }

    private String createInsertSql(String tableName, Map<String, Object> sourceRecord) {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        sourceRecord.forEach((column, value) -> {
            if (!ROW_LOG_ID.equals(column)) {
                columns.add(column);
                values.add(MySqlUtils.toSqlValue(value));
            }
        });

        String columnSql = Joiner.on(",").join(columns);
        String valueSql = Joiner.on(",").join(values);
        return String.format(INSERT_SQL, MySqlUtils.toSqlField(tableName), columnSql, valueSql);
    }

    private String createUpdateSql(Map<String, Object> sourceRecord, Map<String, Object> targetRecord, String tableName, String onUpdateColumn, String uniqueIndex) {
        List<String> setFields = new ArrayList<>();
        sourceRecord.forEach((column, value) -> {
            if (!ROW_LOG_ID.equals(column)) {
                String setField = MySqlUtils.toSqlField(column) + EQUAL_SYMBOL + MySqlUtils.toSqlValue(value);
                setFields.add(setField);
            }
        });
        String setFiledSql = Joiner.on(",").join(setFields);

        StringBuilder whereCondition = new StringBuilder();
        String uniqueIndexCondition = MySqlUtils.toSqlField(uniqueIndex) + EQUAL_SYMBOL + MySqlUtils.toSqlValue(sourceRecord.get(uniqueIndex));
        String onUpdateCondition = onUpdateColumn + EQUAL_SYMBOL + MySqlUtils.toSqlValue(targetRecord.get(onUpdateColumn.replace(MARKS, "")));
        whereCondition.append(uniqueIndexCondition).append(" AND ").append(onUpdateCondition);

        return String.format(UPDATE_SQL, MySqlUtils.toSqlField(tableName), setFiledSql, whereCondition);
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
        List<ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>>> futures = new ArrayList<>();
        for (ConflictRowsLogTbl rowLog : conflictRowsLogTbls) {
            String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
            List<String> onUpdateColumns = onUpdateColumnMap.getOrDefault(tableName, new ArrayList<>());
            ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>> future = executorService.submit(() ->
                    queryRecords(rowLog, srcMhaName, dstMhaName, columnSize, onUpdateColumns, columnsFilerPair.getLeft(), columnsFilerPair.getRight()));
            futures.add(future);
        }

        boolean recordIsEqual = true;
        Map<String, List<Map<String, Object>>> srcResultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> dstResultMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> srcColumnMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> dstColumnMap = new HashMap<>();
        for (ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>> future : futures) {
            try {
                Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> resultPair = future.get(10, TimeUnit.SECONDS);
                recordIsEqual &= resultPair.getLeft();
                Map<String, Object> srcResult = resultPair.getRight().getLeft();
                Map<String, Object> dstResult = resultPair.getRight().getRight();

                extractRecords(srcResultMap, srcColumnMap, srcResult);
                extractRecords(dstResultMap, dstColumnMap, dstResult);
            } catch (Exception e) {
                logger.error("query records error: {}", e);
                throw ConsoleExceptionUtils.message(e.getMessage());
            }
        }

        List<DbReplicationView> dbReplicationViews = drcBuildServiceV2.getDbReplicationView(dstMhaName, srcMhaName);
        List<Map<String, Object>> srcRecords = new ArrayList<>();
        List<Map<String, Object>> dstRecords = new ArrayList<>();
        srcResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = srcColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            resultMap.put("tableName", tableName);

            Long dbReplicationId = getDbReplicationIdByTableName(tableName, dbReplicationViews);
            boolean doubleSync = dbReplicationId != null;
            resultMap.put("doubleSync", doubleSync);
            srcRecords.add(resultMap);
        });
        dstResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = dstColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            resultMap.put("tableName", tableName);

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

    private Map<String, String> getUniqueIndex(List<ConflictRowsLogTbl> conflictRowsLogTbls, String mhaName) {
        List<String> tableNames = conflictRowsLogTbls.stream().map(rowLog -> rowLog.getDbName() + "." + rowLog.getTableName()).distinct().collect(Collectors.toList());
        List<ListenableFuture<Pair<String, String>>> futures = new ArrayList<>();
        for (String tableName : tableNames) {
            ListenableFuture<Pair<String, String>> future = executorService.submit(() -> queryUniqueIndex(mhaName, tableName));
            futures.add(future);
        }

        Map<String, String> uniqueIndexMap = new HashMap<>();
        for (ListenableFuture<Pair<String, String>> future : futures) {
            try {
                Pair<String, String> resultPair = future.get(5, TimeUnit.SECONDS);
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

    private Pair<String, String> queryUniqueIndex(String mha, String tableName) {
        String[] tables = tableName.split("\\.");
        String indexColumn = mysqlService.getFirstUniqueIndex(mha, tables[0], tables[1]);
        return Pair.of(tableName, indexColumn);
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
        return conflictTrxLogTbl;
    }

    private List<ConflictRowsLogTbl> buildConflictRowsLogs(long conflictTrxLogId, ConflictTransactionLog trxLog, Map<String, Long> mhaMap, Map<Long, String> dcMap) {
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

            return target;
        }).collect(Collectors.toList());

        return conflictTrxLogTbls;
    }

    private void extractRecords(Map<String, List<Map<String, Object>>> resultMap, Map<String, List<Map<String, Object>>> columnMap, Map<String, Object> result) {
        String tableName = String.valueOf(result.get("tableName"));
        List<Map<String, Object>> recordList = (List<Map<String, Object>>) result.get("record");
        if (resultMap.containsKey(tableName)) {
            resultMap.get(tableName).addAll(recordList);
        } else {
            resultMap.put(tableName, recordList);
            List<Map<String, Object>> columns = (List<Map<String, Object>>) result.get("metaColumn");
            columnMap.put(tableName, columns);
        }
    }

    private Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> queryRecords(ConflictRowsLogTbl rowLog,
                                                                                       String srcMhaName,
                                                                                       String dstMhaName,
                                                                                       int columnSize,
                                                                                       List<String> onUpdateColumns,
                                                                                       List<DbReplicationView> dbReplicationViews,
                                                                                       Map<Long, List<String>> columnsFieldMap) {
        String sql = StringUtils.isNotBlank(rowLog.getHandleSql()) ? rowLog.getHandleSql() : rowLog.getRawSql();
        Map<String, Object> srcResultMap;
        Map<String, Object> dstResultMap;
        try {
            srcResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(srcMhaName, sql, onUpdateColumns, columnSize));
            dstResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(dstMhaName, sql, onUpdateColumns, columnSize));
        } catch (Exception e) {
            throw ConsoleExceptionUtils.message(e.getMessage());
        }
        boolean sameRecord = recordIsEqual(srcResultMap, dstResultMap, columnsFieldMap, dbReplicationViews);

        List<Map<String, Object>> srcRecords = (List<Map<String, Object>>) srcResultMap.get("record");
        List<Map<String, Object>> dstRecords = (List<Map<String, Object>>) dstResultMap.get("record");
        if (!CollectionUtils.isEmpty(srcRecords)) {
            srcRecords.get(0).put(ROW_LOG_ID, rowLog.getId());
        }
        if (!CollectionUtils.isEmpty(dstRecords)) {
            dstRecords.get(0).put(ROW_LOG_ID, rowLog.getId());
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
            return true;
        }
        if (CollectionUtils.isEmpty(srcRecords) || CollectionUtils.isEmpty(dstRecords)) {
            return false;
        }
        // `db`.`table`
        String tableName = (String) srcResultMap.get("tableName");
        List<String> columns = (List<String>) srcResultMap.get("columns");
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
        return recordIsEqual(columns, srcRecord, dstRecord);
    }

    private void setFilterColumnTip(List<Map<String, Object>> metaColumns, List<String> filterColumns) {
        for (Map<String, Object> metaColumn : metaColumns) {
            String column = (String) metaColumn.get("key");
            if (filterColumns.contains(column)) {
                metaColumn.put("title", column + " (字段过滤)");
            }
        }
    }

    private boolean recordIsEqual(List<String> columns, Map<String, Object> srcRecord, Map<String, Object> dstRecord) {
        for (String column : columns) {
            Object srcValue = srcRecord.get(column);
            Object dstValue = dstRecord.get(column);
            if (srcValue == null && dstValue == null) {
                continue;
            }
            if (srcValue == null || dstValue == null) {
                return false;
            }
            if (!srcValue.equals(dstValue)) {
                return false;
            }
        }
        return true;
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
}
