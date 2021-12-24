package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:24.
 */
public class FloatTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String FLOAT = "insert into `drc4`.`float_type` (`id`, `real`, `real10_4`, `double`, `double10_4`, `float`, `float10_4`, `decimal_m_max_d_min_positive`, `decimal_m_max_d_min_nagetive`, `decimal_d_max_positive`, `decimal_d_max_nagetive`, `decimal_m_max_d_max_positive`, `decimal_m_max_d_max_nagetive`, `decimal_positive_max`, `decimal_positive_min`, `decimal_nagetive_max`, `decimal_nagetive_min`, `numeric`, `numeric10_4`, `datachange_lasttime`)values (\n" +
            "1, -123456789.123456789, 123456.123456789,\n" +
            "-123456789.123456789, 123456.123456789,\n" +
            "-123456789.123456789, 123456.123456789,\n" +
            "11111111112222222222333333333344444444445555555555666666666677777,\n" +
            "-11111111112222222222333333333344444444445555555555666666666677777,\n" +
            "0.111111111122222222223333333333,\n" +
            "-0.111111111122222222223333333333,\n" +
            "11111111112222222222333333333344444.111111111122222222223333333333,\n" +
            "-11111111112222222222333333333344444.111111111122222222223333333333,\n" +
            "99999999999999999999999999999999999999999999999999999999999999999,\n" +
            "0.000000000000000000000000000001,\n" +
            "-0.000000000000000000000000000001,\n" +
            "-99999999999999999999999999999999999999999999999999999999999999999,\n" +
            "-123456789.123456789, 123456.123456789, '2019-10-16 15:15:15.666615'\n" +
            ");";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            FLOAT
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`float_type` set `double` = 222 where `id` = 1;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`float_type` where `id` = 1;"
    );

    private static final String SELECT_FLOAT = "select * from `drc4`.`float_type` where `real` = '-123456789.123456789';";

    public FloatTypePairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_FLOAT);
    }
}
