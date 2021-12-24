package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.cases.DeleteCase;
import com.ctrip.framework.drc.monitor.function.cases.truncate.DefaultTableTruncate;
import com.ctrip.framework.drc.monitor.function.cases.truncate.TableTruncate;
import com.ctrip.framework.drc.monitor.function.execution.delete.SingleDeleteExecution;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.performance.QPSTestCase;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.util.List;

public class UniLateralTruncateCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String TRUNCATE1 = "truncate table `test`.`customer`;";

    int counter = 0;

    private final int FIVE_MINIUTES = 300;

    @Override
    protected String getFirstStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> getRestStatement() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<String> getStatements(DmlTypeEnum dmlType) {
        return null;
    }

    @Override
    protected void doPerformanceTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        if(doTimer()) {
            doWrite(src);
        }
    }

    private boolean doTimer() {
        counter++;
        if(counter >= FIVE_MINIUTES) {
            counter = 0;
            return true;
        }
        return false;
    }


    @Override
    protected void doWrite(ReadWriteSqlOperator src) {
        List<String> truncates = Lists.newArrayList();
        truncates.add(TRUNCATE1);
        for (String s : truncates) {
            logger.info("[Truncate] {} begin", s);
            Execution deleteExecution = new SingleDeleteExecution(s);
            DeleteCase deleteCase = new QPSTestCase(deleteExecution);
            deleteCase.executeDelete(src);
            logger.info("[Truncate] {} end", s);
        }
    }

    public void doWrite(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {
        List<String> truncates = Lists.newArrayList();
        truncates.add(TRUNCATE1);
        for (String truncate : truncates) {
            TableTruncate tableTruncate = new DefaultTableTruncate();
            logger.info("[Truncate] {} begin", truncate);
            tableTruncate.truncateTable(src, truncate);
            tableTruncate.truncateTable(dst, truncate);
            logger.info("[Truncate] {} end", truncate);
        }
    }
}
