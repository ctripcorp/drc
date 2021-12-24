package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.entity.TrafficEntity;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.cases.SelectCase;
import com.ctrip.framework.drc.monitor.function.cases.WriteCase;
import com.ctrip.framework.drc.monitor.function.cases.insert.MultiStatementWriteCase;
import com.ctrip.framework.drc.monitor.function.cases.select.SingleTableSelectCase;
import com.ctrip.framework.drc.monitor.function.execution.insert.MultiStatementWriteExecution;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.performance.AbstractBenchmarkCase;
import com.ctrip.framework.drc.monitor.utils.ResultCompareMonitorReport;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.ctrip.framework.drc.monitor.utils.enums.StatusEnum;
import com.ctrip.xpipe.tuple.Pair;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

/**
 * @Author limingdong
 * @create 2020/3/19
 */
public abstract class AbstractDdlPairCase extends AbstractBenchmarkCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    protected static final String INSERT = "insert";

    protected static final String UPDATE = "update";

    protected static final String DELETE = "delete";

    protected List<String> ddls;

    protected String currentSql;

    protected volatile boolean goOn = true;

    private static final String integrationTestInstanceName = ConfigService.getInstance().getIntegrationTestInstanceName();

    protected ResultCompareMonitorReport insertResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.INSERT, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport updateResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.UPDATE, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport deleteResultCompareMonitorReportSuccess = getResultCompareMonitorReport(DmlTypeEnum.DELETE, StatusEnum.SUCCESS, getClass().getSimpleName());
    protected ResultCompareMonitorReport insertResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.INSERT, StatusEnum.FAIL, getClass().getSimpleName());
    protected ResultCompareMonitorReport updateResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.UPDATE, StatusEnum.FAIL, getClass().getSimpleName());
    protected ResultCompareMonitorReport deleteResultCompareMonitorReportFail = getResultCompareMonitorReport(DmlTypeEnum.DELETE, StatusEnum.FAIL, getClass().getSimpleName());

    protected void initResource() {
        try {
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

    protected void reportCount(DmlTypeEnum dmlType, StatusEnum statusEnum) {
        switch (dmlType) {
            case INSERT:
                if(StatusEnum.SUCCESS == statusEnum) {
                    insertResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    insertResultCompareMonitorReportFail.addOneCount();
                }
                break;
            case UPDATE:
                if(StatusEnum.SUCCESS == statusEnum) {
                    updateResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    updateResultCompareMonitorReportFail.addOneCount();
                }
                break;
            case DELETE:
                if(StatusEnum.SUCCESS == statusEnum) {
                    deleteResultCompareMonitorReportSuccess.addOneCount();
                } else {
                    deleteResultCompareMonitorReportFail.addOneCount();
                }
                break;
            default:
                break;
        }
    }

    public AbstractDdlPairCase() {
        ddls = getDdls();
    }

    @Override
    protected boolean doWrite(ReadWriteSqlOperator src) {
        logger.info("[Execute] sql {}", currentSql);
        return execute(src, currentSql);
    }

    @Override
    protected boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        try {
            Execution selectExecution = new SingleSelectExecution(getQuerySql());
            SelectCase selectCase = new SingleTableSelectCase(selectExecution);
            ReadResource expected = selectCase.executeSelect(src);
            ReadResource actual = selectCase.executeSelect(dst);

            Pair<Boolean, Integer> res = toCompare(expected.getResultSet(), actual.getResultSet(), selectExecution);
            try {
                expected.close();
                actual.close();
            } catch (Exception e) {
                logger.error("[Close] ResultSet error", e);
            }
            if (res.getKey()) {
                logger.info(">>>>>>>>^^__^^>>>>>>>>[Ddl] test RIGHT");
            } else {
                goOn = false;
                logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>[Ddl] test WRONG and set goOn to false");
            }

            System.out.println(res.getKey());
            return res.getKey();
        } catch (Exception e) {
            logger.error("Execute {} error", getQuerySql(), e);
        }

        return false;
    }

    protected boolean doWriteFix(ReadWriteSqlOperator src, DmlTypeEnum dmlType) {
        logger.info("[Execute] sql {}", getFixDmlSql(dmlType));
        return execute(src, getFixDmlSql(dmlType));
    }

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        if (!goOn) {
            logger.info("[goOn] is false and return");
        }
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

        boolean insertResult = true;
        boolean updateResult = true;
        boolean deleteResult = true;

        for (int i = 0; i < ddls.size(); ++i) {
            currentSql = ddls.get(i);
            if (currentSql.toLowerCase().startsWith(INSERT)) {
                insertResult = insertResult && doDdlTest(src, dst, DmlTypeEnum.INSERT);
            } else if(currentSql.toLowerCase().startsWith(UPDATE)) {
                updateResult = updateResult && doDdlTest(src, dst, DmlTypeEnum.UPDATE);
            } else if(currentSql.toLowerCase().startsWith(DELETE)) {
                deleteResult = deleteResult && doDdlTest(src, dst, DmlTypeEnum.DELETE);
            } else{  //ddl
                doWrite(src);
                doWrite(dst);
            }
        }

        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }


    protected boolean reportDmlResult(ReadWriteSqlOperator src, ReadWriteSqlOperator dst, DmlTypeEnum dmlType) {
        sleep(0.5f);
        boolean result = doTest(src, dst);
        if(result) {
            reportCount(dmlType, StatusEnum.SUCCESS);
        } else {
            reportCount(dmlType, StatusEnum.FAIL);
        }
        return result;
    }

    protected boolean doDdlTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst, DmlTypeEnum dmlType) {
        Transaction t = Cat.newTransaction(integrationTestInstanceName + ".test.case", getClass().getSimpleName());
        try {
            doWrite(src);
            if(reportDmlResult(src, dst, dmlType)) {
                t.addData(dmlType.getDescription(), StatusEnum.SUCCESS.getDescription());
                return true;
            } else {
                t.addData(dmlType.getDescription(), StatusEnum.FAIL.getDescription());
                return false;
            }
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            t.setStatus(e);
        } finally {
            t.complete();
        }
        return false;
    }

    protected void testSingleDdl(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        try {
            if(doWriteFix(src, DmlTypeEnum.INSERT)) {
                reportDmlResult(src, dst, DmlTypeEnum.INSERT);
            }
            if(doWriteFix(src, DmlTypeEnum.INSERT)) {
                reportDmlResult(src, dst, DmlTypeEnum.INSERT);
            }
            if(doWriteFix(src, DmlTypeEnum.UPDATE)) {
                reportDmlResult(src, dst, DmlTypeEnum.UPDATE);
            }
            if(doWriteFix(src, DmlTypeEnum.DELETE)) {
                reportDmlResult(src, dst, DmlTypeEnum.DELETE);
            }
        } catch (Exception e) {
            logger.error(getClass().getSimpleName() +"test single ddl error");
        }
    }


    protected boolean doTestSingleDdl(ReadWriteSqlOperator src, ReadWriteSqlOperator dst, DmlTypeEnum dmlType) {
        Transaction t = Cat.newTransaction(integrationTestInstanceName + ".test.case", getClass().getSimpleName());
        try {
            doWriteFix(src, dmlType);
            if(reportDmlResult(src, dst, dmlType)) {
                t.addData(dmlType.getDescription(), StatusEnum.SUCCESS.getDescription());
                return true;
            } else {
                t.addData(dmlType.getDescription(), StatusEnum.FAIL.getDescription());
                return false;
            }
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logError(e);
            t.setStatus(e);
        } finally {
            t.complete();
        }
        return false;
    }

    private List<String> getDdls() {
        List<String> ddls = Lists.newArrayList();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(getSqlFile());
        try(Scanner scanner = new Scanner(is)) {
            StringBuilder stringBuilder = new StringBuilder();
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (StringUtils.isBlank(line)) {
                    ddls.add(stringBuilder.toString());
                    stringBuilder.delete(0, stringBuilder.length());
                } else {
                    stringBuilder.append(line);
                }
            }
            if (stringBuilder.length() > 0) {
                ddls.add(stringBuilder.toString());
            }
        } catch (Exception e) {
            logger.error("read generic-ddl.sql error" ,e);
        }

        return ddls;
    }


    protected boolean execute(ReadWriteSqlOperator src, String sql) {
        Execution execution = new MultiStatementWriteExecution(Lists.newArrayList(sql));
        WriteCase writeCase = new MultiStatementWriteCase(execution);
        return writeCase.executeWrite(src);
    }

    protected ResultCompareMonitorReport getResultCompareMonitorReport(DmlTypeEnum dmlType, StatusEnum statusEnum, String functionType) {
        TrafficEntity trafficEntity = new TrafficEntity.Builder()
                .clusterAppId(100024819L)
                .buName("ddl")
                .dcName(integrationTestInstanceName)
                .clusterName(functionType)
                .ip("10.2.66.144")
                .port(1234)
                .direction(statusEnum.getDescription())
                .module(dmlType.getDescription())
                .build();
        return new ResultCompareMonitorReport(100023500L, trafficEntity);
    }

    protected abstract String getSqlFile();

    protected abstract String getQuerySql();

    protected abstract String getFixInsert();

    protected abstract String getFixUpdate();

    protected abstract String getFixDelete();

    protected String getFixDmlSql(DmlTypeEnum dmlType) {
        switch (dmlType) {
            case INSERT:
                return getFixInsert();
            case UPDATE:
                return getFixUpdate();
            case DELETE:
                return getFixDelete();
            default:
                break;
        }
        return null;
    }

}
