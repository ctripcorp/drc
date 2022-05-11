package com.ctrip.framework.drc.console.service.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.ConflictTransactionLog;
import com.ctrip.framework.drc.console.dto.LogDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.service.LogService;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalQueryDao;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeUpdateSqlBuilder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;

/**
 * Created by jixinwang on 2020/6/22
 */
// TODO 对于公有云云需要往回传
@Service
public class LogServiceImpl implements LogService {
    private static final String SELECT_SQL = "SELECT * FROM %s WHERE %s";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final ExecutorService readerPool = Executors.newFixedThreadPool(3);

    @Autowired
    private DalQueryDao queryDao;

    @Autowired
    private ApplierUploadLogTblDao dao;

    @Autowired
    private ConflictLogDao conflictLogDao;

    @Autowired
    private MhaTblDao mhaTblDao;

    @Autowired
    private DcTblDao dcTblDao;

    @Autowired
    private MhaGroupTblDao mhaGroupTblDao;

    @Autowired
    private MachineTblDao machineTblDao;
    
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;

    @Override
    public void uploadConflictLog(List<ConflictTransactionLog> conflictTransactionLogList) {

        List<ConflictLog> conflictLogList = getConflictLogPojoList(conflictTransactionLogList);
        try {
            conflictLogDao.insert(conflictLogList);
        } catch (Exception e) {
            logger.error("SQLException ", e);
        }
    }

    @Override
    public Map<String, Object> getConflictLog(int pageNo, int pageSize, String keyWord) {
        Map<String, Object> map = new HashMap<>();
        try {
            int total = conflictLogDao.count(keyWord);
            List<ConflictLog> conflictLogs = conflictLogDao.queryLogByPage(pageNo, pageSize, keyWord, null);
            map.put("list", conflictLogs);
            map.put("count", total);
            return map;
        } catch (Exception e) {
            logger.error("get conflict log error", e);
            return null;
        }
    }

    @Override
    public Map<String, Object> getCurrentRecord(long primaryKey) throws Exception {
        ConflictLog conflictLog = conflictLogDao.queryByPk(primaryKey);
        String rawSql = conflictLog.getRawSqlList();
        String srcMhaName = conflictLog.getSrcMhaName();
        String destMhaName = conflictLog.getDestMhaName();
        List<Map> srcTableItems = new ArrayList<>();
        List<Map> destTableItems = new ArrayList<>();
        String[] arr = rawSql.split("\n");
        int totalLen = arr.length;

        Map<String, Object> ret = new HashMap<>();
        boolean diffTransaction = false;
        if (totalLen >= Constants.three) {
            CountDownLatch latch = new CountDownLatch(Constants.three);
            List<Future<Map<String, Object>>> futures = new ArrayList<>();
            List<List<String>> tasks = nDivide(arr, 3);
            for (List<String> task : tasks) {
                logger.info("[[tag=conflictLog]] submit task srcMhaName:{},destMhaName:{}",srcMhaName,destMhaName);
                Future<Map<String, Object>> recordFuture = readerPool.
                        submit(new RecordReader(latch, task, srcMhaName, destMhaName));
                futures.add(recordFuture);
            }
            
            boolean await = latch.await(defaultConsoleConfig.getConflictMhaRecordSearchTime(), TimeUnit.SECONDS);
            if (!await) {
                logger.warn("[[tag=conflictLog]] latch await timeout,something fail in readRecord");
            }

            for (Future<Map<String, Object>> future : futures) {
                Map<String, Object> subResult = future.get(5,TimeUnit.SECONDS);
                if (subResult == null) {
                    future.cancel(true);
                    logger.warn("[[tag=conflictLog]] task execute timeout,cancel");
                } else {
                    srcTableItems.addAll((List<Map>) subResult.get("srcTableItems"));
                    destTableItems.addAll((List<Map>) subResult.get("destTableItems"));
                }
                if (!diffTransaction) diffTransaction = (boolean) subResult.get("diffTransaction");
            }

        } else { // use single thread
            List<String> task = Lists.newArrayList(arr);
            Future<Map<String, Object>> submit = readerPool.submit(() -> readRecord(task, srcMhaName, destMhaName));
            Map<String, Object> result = submit.get();
            srcTableItems.addAll((List<Map>) result.get("srcTableItems"));
            destTableItems.addAll((List<Map>) result.get("destTableItems"));
            diffTransaction = (boolean) result.get("diffTransaction");
        }


        ret.put("rawSqlList", conflictLog.getRawSqlList());
        ret.put("rawSqlExecutedResultList", conflictLog.getRawSqlExecutedResultList());
        ret.put("destCurrentRecordList", conflictLog.getDestCurrentRecordList());
        ret.put("conflictHandleSqlList", conflictLog.getConflictHandleSqlList());
        ret.put("conflictHandleSqlExecutedResultList", conflictLog.getConflictHandleSqlExecutedResultList());
        ret.put("lastResult", conflictLog.getLastResult());

        ret.put("srcDc", getDcNameByMhaName(srcMhaName));
        ret.put("destDc", getDcNameByMhaName(destMhaName));
        ret.put("srcMhaName", srcMhaName);
        ret.put("destMhaName", destMhaName);

        ret.put("srcTableItems", srcTableItems);
        ret.put("destTableItems", destTableItems);
        ret.put("diffTransaction", diffTransaction);

        return ret;

    }

    // len should >= n
    private List<List<String>> nDivide(String[] arr, int n) {
        int len = arr.length;
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < n; i++)
            result.add(new ArrayList<>());

        int i = 0;
        while (i < len) {
            int index = i % n;
            result.get(index).add(arr[i]);
            i++;
        }
        return result;
    }

    class RecordReader implements Callable {
        private final CountDownLatch countDownLatch;

        private final List<String> conflictLogs;

        private final String srcMhaName;

        private final String destMhaName;

        public RecordReader(CountDownLatch countDownLatch, List<String> conflictLogs, String srcMhaName, String destMhaName) {
            this.countDownLatch = countDownLatch;
            this.conflictLogs = conflictLogs;
            this.srcMhaName = srcMhaName;
            this.destMhaName = destMhaName;
        }

        @Override
        public Map<String, Object> call() {
            logger.info("[[tag=conflictLog]] call start");
            Map<String, Object> result = readRecord(conflictLogs, srcMhaName, destMhaName);
            countDownLatch.countDown();
            logger.info("[[tag=conflictLog]] countDownLath countDown one " + Thread.currentThread().getName());
            return result;
        }
    }

    private Map<String, Object> readRecord(List<String> conflictLogs, String srcMhaName, String destMhaName) {
        boolean diffTransaction = false;
        List<Map> srcTableItems = new ArrayList<>();
        List<Map> destTableItems = new ArrayList<>();
        for (String sql : conflictLogs) {
            Map<String, Object> srcTableItem = new HashMap<>();
            Map srcMap = selectRecord(srcMhaName, destMhaName, sql);
            if (srcMap == null) {
                continue;
            }
            srcTableItem.put("column", srcMap.get("column"));
            srcTableItem.put("record", srcMap.get("record"));
            srcTableItems.add(srcTableItem);

            Map<String, Object> destTableItem = new HashMap<>();
            Map destMap = selectRecord(destMhaName, srcMhaName, sql);
            if (destMap == null) {
                continue;
            }
            destTableItem.put("column", destMap.get("column"));
            destTableItem.put("record", destMap.get("record"));
            destTableItems.add(destTableItem);

            if (!diffTransaction && markTableDataDiff(srcMap, destMap)) {
                diffTransaction = true;
            }
        }

        Map<String, Object> ret = new HashMap<>();
        ret.put("srcTableItems", srcTableItems);
        ret.put("destTableItems", destTableItems);
        ret.put("diffTransaction", diffTransaction);

        return ret;
    }

    public void uploadSampleLog(LogDto logDto) {
        ApplierUploadLogTbl daoPojo = createPojo(logDto.getSrcDcName(), logDto.getDestDcName(), logDto.getClusterName(),
                logDto.getUserId(), "sample", logDto.getSqlStatement(), logDto.getSqlHandleConflict());
        try {
            dao.insert(new DalHints(), daoPojo);
        } catch (Exception e) {
            logger.error("SQLException ", e);
        }
    }

    public void deleteLog(DalHints hints) {
        hints = DalHints.createIfAbsent(hints);
        FreeUpdateSqlBuilder builder = new FreeUpdateSqlBuilder();
        builder.setTemplate("delete from applier_upload_log_tbl");
        StatementParameters parameters = new StatementParameters();
        try {
            queryDao.update(builder, parameters, hints);
        } catch (SQLException e) {
            logger.error("SQLException ", e);
        }
    }

    private ApplierUploadLogTbl createPojo(String srcDcName, String destDcName, String clusterName, String uid, String logType, String sqlStatement, String sqlHandleConflict) {
        ApplierUploadLogTbl daoPojo = new ApplierUploadLogTbl();
        daoPojo.setSrcDcName(srcDcName);
        daoPojo.setDestDcName(destDcName);
        daoPojo.setClusterName(clusterName);
        daoPojo.setUid(uid);
        daoPojo.setLogType(logType);
        daoPojo.setSqlStatement(sqlStatement);
        daoPojo.setSqlHandleConflict(sqlHandleConflict);
        return daoPojo;
    }

    @Override
    public Map<String, Object> getLogs(int pageNo, int pageSize) {
        Map<String, Object> map = new HashMap<>();
        try {
            int total = dao.count();
            List<ApplierUploadLogTbl> logs = dao.queryLogByPage(pageNo, pageSize, null);
            map.put("list", logs);
            map.put("count", total);
            return map;
        } catch (Exception e) {
            logger.error("SQLException ", e);
            return null;
        }
    }

    @Override
    public Map<String, Object> getLogs(int pageNo, int pageSize, String keyWord) {
        if (keyWord.equals("")) {
            return getLogs(pageNo, pageSize);
        }
        Map<String, Object> map = new HashMap<>();
        try {
            int total = dao.count(keyWord);
            List<ApplierUploadLogTbl> logs = dao.queryLogByPage(pageNo, pageSize, keyWord, null);
            map.put("list", logs);
            map.put("count", total);
            return map;
        } catch (Exception e) {
            logger.error("SQLException ", e);
            return null;
        }
    }
    

    public Map selectRecord(String mhaName0, String mhaName1,String rawSql) {
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = null;
        try {
            Map<String, String> parseResult = parseSql(rawSql);
            String manipulation = parseResult.get("manipulation");
            if ("insert".equalsIgnoreCase(manipulation) || "delete".equalsIgnoreCase(manipulation)) {
                logger.info("[[tag=conflictLog]] select Record could get condition from insert and delete");
                return null;
            }
            writeSqlOperatorWrapper = initSqlOperator(mhaName0,mhaName1);
            String sql = String.format(SELECT_SQL, parseResult.get("tableName"), parseResult.get("equalConditionStr"));
            logger.info("[[tag=conflictLog]] sql:{}",sql);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            ReadResource readResource = writeSqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            return resultSetConvertList(rs);
        } catch (Exception e) {
            logger.error("[[tag=conflictLog]] selectRecord fail", e);
            return null;
        } finally {
            releaseSqlOperator(writeSqlOperatorWrapper);
        }
    }
    
    private void releaseSqlOperator(WriteSqlOperatorWrapper writeSqlOperatorWrapper) {
        if (writeSqlOperatorWrapper != null) {
            try {
                writeSqlOperatorWrapper.stop();
                writeSqlOperatorWrapper.dispose();
            } catch (Exception e) {
                logger.warn("[[tag=conflictLog]] writeSqlOperatorWrapper stop error", e);
            }
        }
    }

    @Override
    public void updateRecord(Map<String, String> updateInfo) throws Exception {
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = null;
        try {
            String mhaName0 = updateInfo.get("mhaName0");
            String mhaName1 = updateInfo.get("mhaName1");
            String sql = updateInfo.get("sql");

            Map<String, String> parseResult = parseSql(sql);
            writeSqlOperatorWrapper = initSqlOperator(mhaName0,mhaName1);
            String manipulation = parseResult.get("manipulation");
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            if ("update".equalsIgnoreCase(manipulation)) {
                writeSqlOperatorWrapper.update(execution);
            }
            if ("insert".equalsIgnoreCase(manipulation)) {
                writeSqlOperatorWrapper.insert(execution);
            }
            if ("delete".equalsIgnoreCase(manipulation)) {
                writeSqlOperatorWrapper.delete(execution);
            }
        } catch (Exception e) {
            logger.error("[[tag=conflictLog]] selectRecord fail", e);
        } finally {
            releaseSqlOperator(writeSqlOperatorWrapper);
        }
    }

    public String getDcNameByMhaName(String mhaName) throws SQLException {
        DcTbl dc = null;
        try {
            MhaTbl mhaSample = new MhaTbl();
            mhaSample.setMhaName(mhaName);
            List<MhaTbl> mhaList = mhaTblDao.queryBy(mhaSample);
            if (null == mhaList || mhaList.size() == 0) {
                logger.error("mhaTbl not exist mhaName is {}", mhaName);
                return null;
            }
            MhaTbl mha = mhaList.get(0);
            long dcId = mha.getDcId();

            DcTbl dcSample = new DcTbl();
            dcSample.setId(dcId);
            List<DcTbl> dcList = dcTblDao.queryBy(dcSample);
            dc = dcList.get(0);
        } catch (SQLException e) {
            logger.error("SQLException occur in getDcNameByMhaName {}", mhaName);
            return null;
        }
        return dc.getDcName();
    }

    public WriteSqlOperatorWrapper initSqlOperator(String mhaName,String anOtherMhaName) throws Exception {
        MhaTbl mhaSample = new MhaTbl();
        mhaSample.setMhaName(mhaName);
        List<MhaTbl> mhaList = mhaTblDao.queryBy(mhaSample);
        if (null == mhaList || mhaList.size() == 0) 
            throw new SQLException("[[tag=conflictLog]] mhaTbl not exist mhaName is " + mhaName);
        MhaTbl mha = mhaList.get(0);
        Long mhaId = mha.getId();
        Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName,anOtherMhaName,BooleanEnum.FALSE);
        if (mhaGroupId == null) {
            mhaGroupId = mha.getMhaGroupId();
        }
        if (mhaGroupId == null) {
            logger.warn("[[tag=conflictLog]] cannt find mhaGroupId for" + mhaName + "-" + anOtherMhaName);
        }
        MhaGroupTbl mhaGroup = mhaGroupTblDao.queryByPk(mhaGroupId);
        String monitorUser = mhaGroup.getMonitorUser();
        String monitorPassword = mhaGroup.getMonitorPassword();

        MachineTbl machineSample = new MachineTbl();
        machineSample.setMhaId(mhaId);
        machineSample.setMaster(1);
        List<MachineTbl> machineList = machineTblDao.queryBy(machineSample);
        MachineTbl machine = machineList.get(0);
        String ip = machine.getIp();
        int port = machine.getPort();

        Endpoint endpoint = new MySqlEndpoint(ip, port, monitorUser, monitorPassword, BooleanEnum.TRUE.getCode().equals(machine.getMaster()));
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
        writeSqlOperatorWrapper.initialize();
        writeSqlOperatorWrapper.start();
        return writeSqlOperatorWrapper;
    }

    public Map<String, String> parseSql(String sql) {
        Map<String, String> parseResult = new HashMap<>();

        String dbType = JdbcConstants.MYSQL;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        String tableName = visitor.getCurrentTable();
        parseResult.put("tableName", tableName);
        Map<TableStat.Name, TableStat> manipulationMap = visitor.getTables();
        String tableNameFormat = tableName.replace("`", "");
        TableStat.Name name = new TableStat.Name(tableNameFormat);
        TableStat stat = manipulationMap.get(name);
        parseResult.put("manipulation", stat.toString());
        List<TableStat.Condition> Conditions = visitor.getConditions();
        TableStat.Condition equalCondition = Conditions.get(0);
        String equalConditionStr = equalCondition.toString();
        parseResult.put("equalConditionStr", equalConditionStr);
        return parseResult;
    }

    public List<ConflictLog> getConflictLogPojoList(List<ConflictTransactionLog> conflictTransactionLogList) {

        List<ConflictLog> conflictLogList = new ArrayList<ConflictLog>();

        for (ConflictTransactionLog conflictTransactionLog : conflictTransactionLogList) {
            ConflictLog daoPojo = new ConflictLog();
            daoPojo.setSrcMhaName(conflictTransactionLog.getSrcMhaName());
            daoPojo.setDestMhaName(conflictTransactionLog.getDestMhaName());
            daoPojo.setClusterName(conflictTransactionLog.getClusterName());
            daoPojo.setRawSqlList(listToString(conflictTransactionLog.getRawSqlList(), '\n'));
            daoPojo.setRawSqlExecutedResultList(listToString(conflictTransactionLog.getRawSqlExecutedResultList(), '\n'));
            daoPojo.setDestCurrentRecordList(listToString(conflictTransactionLog.getDestCurrentRecordList(), '\n'));
            daoPojo.setConflictHandleSqlList(listToString(conflictTransactionLog.getConflictHandleSqlList(), '\n'));
            daoPojo.setConflictHandleSqlExecutedResultList(listToString(conflictTransactionLog.getConflictHandleSqlExecutedResultList(), '\n'));
            daoPojo.setLastResult(conflictTransactionLog.getLastResult());
            daoPojo.setSqlExecuteTime(conflictTransactionLog.getConflictHandleTime());
            conflictLogList.add(daoPojo);
        }
        return conflictLogList;
    }

    public static String listToString(List list, char separator) {
        return StringUtils.join(list.toArray(), separator);
    }

    public static Map<String, Object> resultSetConvertList(ResultSet rs) throws SQLException {
        Map<String, Object> ret = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        List<Map<String, Object>> metaColumn = new ArrayList<>();
        List<String> columnList = new ArrayList<>();
        for (int j = 1; j <= columnCount; j++) {
            Map<String, Object> columnData = new LinkedHashMap<>();
            columnData.put("title", md.getColumnName(j));
            columnData.put("key", md.getColumnName(j));
            columnData.put("width", 200);
            columnData.put("tooltip", true);
            metaColumn.add(columnData);
            columnList.add(md.getColumnName(j));
        }
        ret.put("column", metaColumn);
        ret.put("columnList", columnList);

        while (rs.next()) {
            Map<String, Object> rowData = new LinkedHashMap<>();
            for (String columnName : columnList) {
                rowData.put(columnName, rs.getString(columnName));
            }

            Map<String, String> cellClassName = new HashMap<>();
            rowData.put("cellClassName", cellClassName);
            list.add(rowData);
        }
        ret.put("record", list);
        return ret;
    }

    public boolean markTableDataDiff(Map srcMap, Map destMap) {
        List<String> srcColumnList = (List<String>) srcMap.get("columnList");
        List<String> destColumnList = (List<String>) destMap.get("columnList");

        List<Map<String, Object>> srcRecordList = (List<Map<String, Object>>) srcMap.get("record");
        List<Map<String, Object>> destRecordList = (List<Map<String, Object>>) destMap.get("record");

        if (srcRecordList.isEmpty() && destRecordList.isEmpty()) {
            return false;
        }
        if ((srcRecordList.isEmpty() && !destRecordList.isEmpty()) || (!srcRecordList.isEmpty() && destRecordList.isEmpty())) {
            return true;
        }

        Map<String, Object> srcRecord = srcRecordList.get(0);
        Map<String, Object> destRecord = destRecordList.get(0);

        //srcColumnList - destColumnList
        List<String> srcAndDestDiff = srcColumnList.stream().filter(item -> !destColumnList.contains(item)).collect(toList());

        //destColumnList - srcColumnList
        List<String> destAndSrcDiff = destColumnList.stream().filter(item -> !srcColumnList.contains(item)).collect(toList());

        boolean srcExtraColumn = markExtraColumn(srcRecord, srcAndDestDiff);
        boolean destExtraColumn = markExtraColumn(destRecord, destAndSrcDiff);

        //the intersection of src and dest
        List<String> intersectionList = srcColumnList.stream().filter(item -> destColumnList.contains(item)).collect(toList());

        boolean recordDiff = markDiffRecord(srcRecord, destRecord, intersectionList);

        return srcExtraColumn || destExtraColumn || recordDiff;
    }


    public boolean markExtraColumn(Map<String, Object> record, List<String> columnDiff) {
        List<String> extraColumnList = new ArrayList<>();
        for (Map.Entry<String, Object> entry1 : record.entrySet()) {
            if (columnDiff.contains(entry1.getKey())) {
                extraColumnList.add(entry1.getKey());
            }
        }

        Map<String, String> cellClassName = (Map<String, String>) record.get("cellClassName");
        for (String extraColumn : extraColumnList) {
            cellClassName.put(extraColumn, "table-info-cell-extra-column-add");
        }
        return !extraColumnList.isEmpty();
    }

    public boolean markDiffRecord(Map<String, Object> srcRecord, Map<String, Object> destRecord, List<String> intersectionList) {
        List<String> diffColumnList = new ArrayList<>();
        for (String columnName : intersectionList) {
            Object srcRecordValue = srcRecord.get(columnName);
            Object destRecordValue = destRecord.get(columnName);
            if (srcRecordValue == null && destRecordValue == null) {
                continue;
            }
            if ((srcRecordValue == null && destRecordValue != null) || (srcRecordValue != null && destRecordValue == null)) {
                diffColumnList.add(columnName);
                continue;
            }
            if ((srcRecordValue != null) && (!srcRecordValue.equals(destRecordValue))) {
                diffColumnList.add(columnName);
            }
        }

        Map<String, String> srcCellClassName = (Map<String, String>) srcRecord.get("cellClassName");
        Map<String, String> destCellClassName = (Map<String, String>) destRecord.get("cellClassName");
        for (String diffColumn : diffColumnList) {
            srcCellClassName.put(diffColumn, "table-info-cell-extra-column-diff");
            destCellClassName.put(diffColumn, "table-info-cell-extra-column-diff");
        }

        return !diffColumnList.isEmpty();
    }
}
