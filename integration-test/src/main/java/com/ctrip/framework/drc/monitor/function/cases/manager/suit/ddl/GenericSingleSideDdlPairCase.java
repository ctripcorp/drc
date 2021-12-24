package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.ctrip.xpipe.tuple.Pair;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2020/3/23
 */
public class GenericSingleSideDdlPairCase extends GenericDdlPairCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

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
            }  else {  //ddl
                doWrite(src);
                /**
                 * test ddl single side
                 */
                if (i > 1 && i < ddls.size() - 8){
                    insertResult = insertResult && doTestSingleDdl(src, dst, DmlTypeEnum.INSERT);
                    updateResult = updateResult && doTestSingleDdl(src, dst, DmlTypeEnum.UPDATE);
                    deleteResult = deleteResult && doTestSingleDdl(src, dst, DmlTypeEnum.DELETE);
                }
                doWrite(dst);
            }
        }

        logger.info(">>>>>>>>>>>> END test [{}] >>>>>>>>>>>>\n", getClass().getSimpleName());
    }


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
                    int columnCount = Math.min(srcColumnCount, dstColumnCount);
                    for (int i = 1; i <= columnCount; ++i) {
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
            logger.error("SQLException error", e);
            alarm.alarm(String.format("%s ResultSet next error %s", getClass().getSimpleName(), selectExecution.getStatements()));
        }
        return Pair.from(false, maxId);
    }

}
