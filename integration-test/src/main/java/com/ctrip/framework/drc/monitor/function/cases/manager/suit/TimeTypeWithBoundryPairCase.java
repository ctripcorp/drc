package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.monitor.function.execution.select.SingleSelectExecution;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.monitor.utils.enums.DmlTypeEnum;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:33.
 */
public class TimeTypeWithBoundryPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String TIME_TYPE_WITH_BOUNDRY = "insert into `drc4`.`time_type_boundary` values (\n1," +
            "'0000-01-01 00:00:00.000000',\n" +
            "'9999-12-31 23:59:59.999999',\n" +
            "\n" +
            "'-838:59:59.000000',\n" +
            "'838:59:59.000000',\n" +
            "'-11:11:11.000000',\n" +
            "'00:00:00.000000',\n" +
            "'11:11:11.000000',\n" +
            "'-838:59:59.000000',\n" +
            "'838:59:59.000000',\n" +
            "\n" +
            "'0000-01-01 00:00:00.000000',\n" +
            "'9999-12-31 23:59:59.999999',\n" +
            "'2019-10-17 11:59:59.999999',\n" +
            "'2019-10-17 23:59:59.999999',\n" +
            "'2019-10-17 01:59:59.999999',\n" +
            "'0000-01-01 00:00:00.000000',\n" +
            "'9999-12-31 23:59:59.999999',\n" +
            "\n" +
            "'1970-01-01 08:00:01.000000',\n" +
            "'2038-01-19 11:14:07.999999',\n" +
            "'2038-01-19 11:14:07.999999',\n" +
            "'2038-01-19 11:14:07.999999',\n" +
            "'2038-01-19 11:14:07.999999',\n" +
            "'1970-01-01 08:00:01.000000',\n" +
            "'2038-01-19 11:14:07.999999',\n" +
            "\n" +
            "'0',\n" +
            "'2115'\n" +
            "'2019-10-16 15:15:15.666614'\n"+
            ");";

    private static final String SELECT_TIME_TYPE_WITH_BOUNDRY = "select * from drc4.time_type_boundary";

    protected List<String> getStatements() {
        return Lists.newArrayList(TIME_TYPE_WITH_BOUNDRY);
    }
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
    protected void doTest(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {

        checkResult(src, dst);
    }

    private void checkResult(ReadWriteSqlOperator src, ReadSqlOperator<ReadResource> dst) {
        Execution selectExecution = new SingleSelectExecution(SELECT_TIME_TYPE_WITH_BOUNDRY);
        diff(selectExecution, src, dst);
    }

}
