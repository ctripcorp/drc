package com.ctrip.framework.drc.console.service.log;

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
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.*;
import com.ctrip.framework.drc.console.vo.v2.DbReplicationView;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
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

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(5, "conflictLog"));

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
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return new ArrayList<>();
        }

        List<Long> conflictTrxLogIds = conflictRowsLogTbls.stream().map(ConflictRowsLogTbl::getConflictTrxLogId).collect(Collectors.toList());
        List<ConflictTrxLogTbl> conflictTrxLogTbls = conflictTrxLogTblDao.queryByIds(conflictTrxLogIds);
        Set<String> mhaNames = new HashSet<>();
        List<String> srcMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getSrcMhaName).distinct().collect(Collectors.toList());
        List<String> dstMhaNames = conflictTrxLogTbls.stream().map(ConflictTrxLogTbl::getDstMhaName).distinct().collect(Collectors.toList());
        mhaNames.addAll(srcMhaNames);
        mhaNames.addAll(dstMhaNames);
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(mhaNames));
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();

        Map<Long, ConflictTrxLogTbl> conflictTrxLogMap = conflictTrxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getId, Function.identity()));
        Map<String, MhaTblV2> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, Function.identity()));
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));

        List<ConflictRowsLogView> views = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogView target = new ConflictRowsLogView();
            BeanUtils.copyProperties(source, target, "handleTime");
            target.setHandleTime(DateUtils.longToString(source.getHandleTime()));
            target.setConflictRowsLogId(source.getId());

            ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogMap.get(source.getConflictTrxLogId());
            target.setGtid(conflictTrxLogTbl.getGtid());
            target.setConflictTrxLogId(conflictTrxLogTbl.getId());
            MhaTblV2 srcMha = mhaMap.get(conflictTrxLogTbl.getSrcMhaName());
            MhaTblV2 dstMha = mhaMap.get(conflictTrxLogTbl.getDstMhaName());
            target.setSrcDc(dcMap.get(srcMha.getDcId()));
            target.setDstDc(dcMap.get(dstMha.getDcId()));
            return target;
        }).collect(Collectors.toList());
        return views;
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
        view.setSrcDc(srcDcTbl.getDcName());
        view.setDstDc(dstTbl.getDcName());

        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }
        List<ConflictRowsLogDetailView> rowsLogDetailViews = conflictRowsLogTbls.stream().map(source -> {
            ConflictRowsLogDetailView target = new ConflictRowsLogDetailView();
            BeanUtils.copyProperties(source, target);
            return target;
        }).collect(Collectors.toList());
        view.setRowsLogDetailViews(rowsLogDetailViews);
        return view;
    }

    @Override
    public ConflictCurrentRecordView getConflictCurrentRecordView(Long conflictTrxLogId) throws Exception {
        ConflictCurrentRecordView view = new ConflictCurrentRecordView();

        ConflictTrxLogTbl conflictTrxLogTbl = conflictTrxLogTblDao.queryById(conflictTrxLogId);
        if (conflictTrxLogTbl == null) {
            return view;
        }
        List<ConflictRowsLogTbl> conflictRowsLogTbls = conflictRowsLogTblDao.queryByTrxLogId(conflictTrxLogId);
        if (CollectionUtils.isEmpty(conflictRowsLogTbls)) {
            return view;
        }
        MhaTblV2 srcMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getSrcMhaName());
        MhaTblV2 dstMha = mhaTblV2Dao.queryByMhaName(conflictTrxLogTbl.getDstMhaName());

        Pair<List<DbReplicationView>, Map<Long, List<String>>> columnsFilerPair = getTableColumnsFilterFields(srcMha.getMhaName(), dstMha.getMhaName());
        Map<String, List<String>> onUpdateColumnMap = getOnUpdateColumns(conflictRowsLogTbls, srcMha.getMhaName());
        List<ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>>> futures = new ArrayList<>();
        for (ConflictRowsLogTbl rowLog : conflictRowsLogTbls) {
            String sql = StringUtils.isNotBlank(rowLog.getHandleSql()) ? rowLog.getHandleSql() : rowLog.getRawSql();

            String tableName = rowLog.getDbName() + "." + rowLog.getTableName();
            List<String> onUpdateColumns = onUpdateColumnMap.getOrDefault(tableName, new ArrayList<>());
            ListenableFuture<Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>>> future = executorService.submit(() ->
                    queryRecords(srcMha.getMhaName(), dstMha.getMhaName(), sql, onUpdateColumns, columnsFilerPair.getLeft(), columnsFilerPair.getRight()));
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
                throw ConsoleExceptionUtils.message("query records error");
            }
        }

        List<Map<String, Object>> srcRecords = new ArrayList<>();
        List<Map<String, Object>> dstRecords = new ArrayList<>();
        srcResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = srcColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            srcRecords.add(resultMap);
        });
        dstResultMap.forEach((tableName, records) -> {
            List<Map<String, Object>> columns = dstColumnMap.get(tableName);
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("columns", columns);
            resultMap.put("records", records);
            dstRecords.add(resultMap);
        });
        view.setSrcRecords(srcRecords);
        view.setDstRecords(dstRecords);
        view.setRecordIsEqual(recordIsEqual);
        return view;
    }

    @Override
    @DalTransactional(logicDbName = "bbzfxdrclogdb_w")
    public void createConflictLog(List<ConflictTransactionLog> trxLogs) throws Exception {
        List<ConflictTrxLogTbl> conflictTrxLogTbls = trxLogs.stream().map(this::buildConflictTrxLog).collect(Collectors.toList());
        conflictTrxLogTbls = conflictTrxLogTblDao.batchInsertWithReturnId(conflictTrxLogTbls);
        Map<String, Long> trxLogMap = conflictTrxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getGtid, ConflictTrxLogTbl::getId));

        List<ConflictRowsLogTbl> conflictRowsLogTbls = new ArrayList<>();
        trxLogs.stream().forEach(trxLog -> {
            Long conflictTrxLogId = trxLogMap.get(trxLog.getGtid());
            List<ConflictRowsLogTbl> conflictRowsLogList = buildConflictRowsLogs(conflictTrxLogId, trxLog);
            conflictRowsLogTbls.addAll(conflictRowsLogList);
        });
        conflictRowsLogTblDao.insert(conflictRowsLogTbls);
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

        conflictTrxLogTblDao.delete(conflictTrxLogTbls);
        conflictRowsLogTblDao.delete(conflictRowsLogTbls);

        return conflictRowsLogTbls.size();
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

    private Pair<String, List<String>> queryOnUpdateColumns(String mha, String tableName) {
        String[] tables = tableName.split("\\.");
        List<String> onUpdateColumns = mysqlService.getAllOnUpdateColumns(mha, tables[0], tables[1]);
        return Pair.of(tableName, onUpdateColumns);
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

    private List<ConflictRowsLogTbl> buildConflictRowsLogs(long conflictTrxLogId, ConflictTransactionLog trxLog) {
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

    private Pair<Boolean, Pair<Map<String, Object>, Map<String, Object>>> queryRecords(String srcMhaName,
                                                                                       String dstMhaName,
                                                                                       String sql,
                                                                                       List<String> onUpdateColumns,
                                                                                       List<DbReplicationView> dbReplicationViews,
                                                                                       Map<Long, List<String>> columnsFieldMap) {
        Map<String, Object> srcResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(srcMhaName, sql, onUpdateColumns));
        Map<String, Object> dstResultMap = mysqlService.queryTableRecords(new QueryRecordsRequest(dstMhaName, sql, onUpdateColumns));
        boolean sameRecord = recordIsEqual(srcResultMap, dstResultMap, columnsFieldMap, dbReplicationViews);

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
                return true;
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
