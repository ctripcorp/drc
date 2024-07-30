package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.alarm.Alarm;
import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.InsertCase;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.ResultCompareMonitorReport;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.ctrip.framework.drc.monitor.utils.enums.StatusEnum;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.Lists;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/12 上午11:41.
 */
public abstract class AbstractMultiWriteInTransactionPairCase extends AbstractPairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private List<String> insertStatementList;

    private List<String> updateStatementList;

    private List<String> deleteStatementList;

    private List<String> selectList = new ArrayList<>();

    protected static final String integrationTestInstanceName = ConfigService.getInstance().getIntegrationTestInstanceName();

    protected ResultCompareMonitorReport insertResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.INSERT, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport updateResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.UPDATE, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport deleteResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.DELETE, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport insertResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.INSERT, StatusEnum.FAIL, getClass().getSimpleName());
    protected ResultCompareMonitorReport updateResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.UPDATE, StatusEnum.FAIL, getClass().getSimpleName());
    protected ResultCompareMonitorReport deleteResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.DELETE, StatusEnum.FAIL, getClass().getSimpleName());

    public void setInsertStatementList(List<String> insertStatementList) {
        this.insertStatementList = insertStatementList;
    }

    public void setUpdateStatementList(List<String> updateStatementList) {
        this.updateStatementList = updateStatementList;
    }

    public void setDeleteStatementList(List<String> deleteStatementList) {
        this.deleteStatementList = deleteStatementList;
    }

    public void setSelectList(String... selectArray) {
        selectList.addAll(Arrays.asList(selectArray));
    }

    protected List<String> getStatements(DmlTypeEnum dmlType) {
        switch (dmlType) {
            case INSERT:
                return insertStatementList;
            case UPDATE:
                return updateStatementList;
            case DELETE:
                return deleteStatementList;
            default:
                return null;
        }
    }

    protected void initResource(List<String> insertStatementList, List<String> updateStatementList,
                                List<String> deleteStatementList, String... selectArray) {
        try {
            setInsertStatementList(insertStatementList);
            setUpdateStatementList(updateStatementList);
            setDeleteStatementList(deleteStatementList);
            setSelectList(selectArray);
            insertResultCompareMonitorReportSuccess.initialize();
            updateResultCompareMonitorReportSuccess.initialize();
            deleteResultCompareMonitorReportSuccess.initialize();
            insertResultCompareMonitorReportFail.initialize();
            updateResultCompareMonitorReportFail.initialize();
            deleteResultCompareMonitorReportFail.initialize();
        } catch (Exception e) {
            logger.error("initialize " + getClass().getSimpleName() + " resultCompareMonitorReport error");
        }
    }

    protected boolean diff(Execution selectExecution, ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        SelectCase selectCase = new SingleTableSelectCase(selectExecution);
        ReadResource expected = selectCase.executeSelect(src);
        ReadResource actual = selectCase.executeSelect(dst);
        boolean res = doDiff(expected.getResultSet(), actual.getResultSet(), selectExecution);
        expected.close();
        actual.close();
        return res;
    }

    private boolean doDiff(ResultSet expected, ResultSet actual, Execution selectExecution) {
        if (expected == null && actual == null) {
            return true;
        } else if ((expected == null && actual != null) || (expected != null && actual == null)) {
            alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
            return false;
        } else {
            return toCompare(expected, actual, selectExecution);
        }

    }

    private boolean toCompare(ResultSet expected, ResultSet actual, Execution selectExecution) {
        Alarm alarm = getAlarm();
        try {
            do {
                boolean sourceHasNext = expected.next();
                boolean destHasNext = actual.next();
                if (sourceHasNext != destHasNext) {
                    alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                    return false;
                }
                if (sourceHasNext && destHasNext) {
                    ResultSetMetaData src = expected.getMetaData();
                    ResultSetMetaData dst = actual.getMetaData();
                    if ((src == null && dst != null) || (src != null && dst == null)) {
                        alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                        return false;
                    }
                    int srcColumnCount = src.getColumnCount();
                    int dstColumnCount = dst.getColumnCount();
                    if (srcColumnCount != dstColumnCount) {
                        alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                        return false;
                    }
                    for (int i = 1; i <= srcColumnCount; ++i) {
                        Object srcObject = expected.getObject(i);
                        Object dstObject = actual.getObject(i);
                        if (srcObject == null && dstObject == null) {
                            continue;
                        }
                        if ((srcObject == null && dstObject != null) || (srcObject != null && dstObject == null)) {
                            alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                            return false;
                        }

                        assert srcObject != null;
                        assert dstObject != null;

                        if (srcObject.getClass().isArray() && dstObject.getClass().isArray()) {
                            if (Array.getLength(srcObject) != Array.getLength(dstObject)) {
                                alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                                return false;
                            }
                            for (int j = 0; j < Array.getLength(srcObject); j++) {
                                if (!Array.get(srcObject, j).equals(Array.get(dstObject, j))) {
                                    alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                                    return false;
                                }
                            }
                            continue;
                        }
                        if (!srcObject.equals(dstObject)) {
                            alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                            return false;
                        }
                    }
                } else {
                    return true;
                }
            } while (true);
        } catch (SQLException e) {
            alarm.alarm(String.format("%s ResultSet next error %s", getClass().getSimpleName(), selectExecution.getStatements()));
        }
        return true;
    }

    protected Alarm getAlarm() {
        return this.alarm;
    }

    protected void parkOneSecond() {
        try {
            Thread.sleep(1000);
            logger.info(">>>>>>>>>>>> mock replicate delay for ONE second\n");
        } catch (InterruptedException e) {
            logger.error("sleep error", e);
        }
        logger.info(">>>>>>>>>>>> mock replicate delay for ONE second\n");
    }

    protected void parkThreeSecond() {
        try {
            Thread.sleep(3000);
            logger.info(">>>>>>>>>>>> mock replicate delay for THREE second\n");
        } catch (InterruptedException e) {
            logger.error("sleep error", e);
        }
        logger.info(">>>>>>>>>>>> mock replicate delay for THREE second\n");
    }

    @Override
    public void test(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

        if (insertStatementList == null) {
            doPerformanceTest(src, dst);
        } else {
            doFunctionTest(src, dst);
        }

        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }

    private void doFunctionTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        doDmlTest(src, dst, DmlTypeEnum.INSERT);
        doDmlTest(src, dst, DmlTypeEnum.UPDATE);
        doDmlTest(src, dst, DmlTypeEnum.DELETE);
    }

    protected void doPerformanceTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {

        doWrite(src);

        doTest(src, dst);
    }

    protected void doWrite(ReadWriteSqlOperator src, DmlTypeEnum dmlType) {

        WriteExecution writeExecution = new MultiStatementWriteExecution(getStatements(dmlType));
        InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
        boolean affected = insertCase.executeInsert(src);
        if (!affected) {
            alarm.alarm(String.format("%s execute %s error %s", getClass().getSimpleName(), dmlType.getDescription(), writeExecution.getStatements()));
        }
        parkOneSecond();

    }

    protected boolean doDmlTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst, DmlTypeEnum dmlType) {
        Transaction t = Cat.newTransaction(integrationTestInstanceName + ".test.case", getClass().getSimpleName());
        try {
            doWrite(src, dmlType);

            boolean result = true;
            for (int i = 0; i < selectList.size(); i++) {
                Execution selectExecution = new SingleSelectExecution(selectList.get(i));
                result = result && diff(selectExecution, src, dst);
                if (!result) {
                    reportCount(dmlType, StatusEnum.FAIL);
                    t.addData(dmlType.getDescription(), StatusEnum.FAIL.getDescription());
                    t.setStatus(new Exception("Not all cases pass"));
                    return false;
                }
            }
            reportCount(dmlType, StatusEnum.SUCCESS);
            t.addData(dmlType.getDescription(), StatusEnum.SUCCESS.getDescription());
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            t.setStatus(e);
        } finally {
            t.complete();
        }
        return true;
    }

    protected ResultCompareMonitorReport getResultCompareMonitorReport(DmlTypeEnum dmlType, StatusEnum statusEnum, String functionType) {
        TrafficEntity trafficEntity = new TrafficEntity.Builder()
                .clusterAppId(100024819L)
                .buName("dml")
                .dcName(integrationTestInstanceName)
                .clusterName(functionType)
                .ip("10.2.66.144")
                .port(1234)
                .direction(statusEnum.getDescription())
                .module(dmlType.getDescription())
                .build();
        return new ResultCompareMonitorReport(100023500L, trafficEntity);
    }

    protected void reportCount(DmlTypeEnum dmlType, StatusEnum statusEnum) {
        switch (dmlType) {
            case INSERT:
                if (StatusEnum.SUCCESS == statusEnum) {
                    insertResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    insertResultCompareMonitorReportFail.addOneCount();
                }
                break;
            case UPDATE:
                if (StatusEnum.SUCCESS == statusEnum) {
                    updateResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    updateResultCompareMonitorReportFail.addOneCount();
                }
                break;
            case DELETE:
                if (StatusEnum.SUCCESS == statusEnum) {
                    deleteResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    deleteResultCompareMonitorReportFail.addOneCount();
                }
                break;
            default:
                break;
        }
    }

    protected void doWrite(ReadWriteSqlOperator src) {
        WriteExecution writeExecution = new MultiStatementWriteExecution(getStatements());
        InsertCase insertCase = new MultiStatementWriteCase(writeExecution);
        boolean affected = insertCase.executeInsert(src);
        if (!affected) {
            alarm.alarm(String.format("%s execute write error %s", getClass().getName(), writeExecution.getStatements()));
        }
        parkOneSecond();
    }

    protected String getFirstStatement() {
        return insertStatementList.get(0);
    }

    protected List<String> getRestStatement() {
        return new ArrayList<>();
    }

    protected List<String> getStatements() {
        String statement = String.format(getFirstStatement(), 0);  //auto increase, set to 0, or conflict
        List<String> copy = Lists.newArrayList(statement);
        copy.addAll(getRestStatement());
        return copy;
    }

    protected void doTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {

    }
}
