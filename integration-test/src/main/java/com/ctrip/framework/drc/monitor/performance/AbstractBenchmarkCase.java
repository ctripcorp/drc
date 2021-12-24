package com.ctrip.framework.drc.monitor.performance;

import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.config.ConfigService;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.xpipe.tuple.Pair;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by mingdongli
 * 2019/11/17 上午11:43.
 */
public abstract class AbstractBenchmarkCase extends AbstractPairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    protected static String getPropertyOrDefault(String name, String defaultValue) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }

        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        logger.info(">>>>>>>>>>>> START test [{}] >>>>>>>>>>>>", getClass().getSimpleName());

        doWrite(src);

        if (ConfigService.getInstance().getBenchmarkTwoSideWriteSwitch()) {
            doWrite(dst);
        }

        doTest(src, dst);
        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }

    protected abstract boolean doWrite(ReadWriteSqlOperator src);

    protected abstract boolean doTest(ReadWriteSqlOperator src, ReadWriteSqlOperator dst);

    @SuppressWarnings("findbugs:NP_NULL_ON_SOME_PATH_MIGHT_BE_INFEASIBLE")
    protected Pair<Boolean, Integer> toCompare(ResultSet expected, ResultSet actual, Execution selectExecution) {
        int maxId = -1;
        try {
            do {
                boolean sourceHasNext = expected.next();
                boolean destHasNext = actual.next();
                if (sourceHasNext != destHasNext) {  //not equal
                    alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                    return Pair.from(false, maxId);
                }
                if (sourceHasNext && destHasNext) {  //both true
                    ResultSetMetaData src = expected.getMetaData();
                    ResultSetMetaData dst = actual.getMetaData();
                    if ((src == null && dst != null) || (src != null && dst == null)) {
                        alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                        return Pair.from(false, maxId);
                    }
                    int srcColumnCount = src.getColumnCount();
                    int dstColumnCount = dst.getColumnCount();
                    if (srcColumnCount != dstColumnCount) {
                        alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                        return Pair.from(false, maxId);
                    }
                    for (int i = 1; i <= srcColumnCount; ++i) {
                        Object srcObject = expected.getObject(i);
                        Object dstObject = actual.getObject(i);

                        if (srcObject == null && dstObject == null) {
                            continue;
                        }

                        if ((srcObject == null && dstObject != null) || (srcObject != null && dstObject == null)) {
                            alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                            return Pair.from(false, maxId);
                        }

                        if (srcObject.getClass().isArray() && dstObject.getClass().isArray()) {
                            if (Array.getLength(srcObject) != Array.getLength(dstObject)) {
                                alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                                return Pair.from(false, maxId);
                            }
                            for (int j = 0; j < Array.getLength(srcObject); j++) {
                                if (!Array.get(srcObject, j).equals(Array.get(dstObject, j))) {
                                    alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                                    return Pair.from(false, maxId);
                                }
                            }
                            continue;
                        }
                        if (!srcObject.equals(dstObject)) {
                            alarm.alarm(String.format("%s src and dst MISMATCH for [%s]", getClass().getSimpleName(), selectExecution.getStatements()));
                            return Pair.from(false, maxId);
                        }
                        if (i == 1) {
                            maxId = (int) srcObject;
                        }
                    }
                } else {  //both false and to the end
                    return Pair.from(true, maxId);
                }
            } while (true);
        } catch (SQLException e) {
            alarm.alarm(String.format("%s ResultSet next error %s", getClass().getSimpleName(), selectExecution.getStatements()));
        }
        return Pair.from(false, maxId);
    }

    protected void sleep(float second) {
        try {
            Thread.sleep((long) (1000 * second));  // delay of executorService.submit
        } catch (InterruptedException e) {
            logger.error("sleep error", e);
        }
    }

}
