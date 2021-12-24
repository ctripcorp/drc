package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:22.
 */
public class MultiTypeNumberUnsignedPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String MAX_NUM = "insert into `drc4`.`multi_type_number_unsigned` values (1, 0, 0, 0, 0, 0, 0, 72057594037927935, 18446744073709551615,\n" +
            "  255, 65535, 16777215, 4294967295, 4294967295, 18446744073709551615, '2019-10-16 15:15:15.666613'\n" +
            ");";

    public static final String MID_NUM = "insert into `drc4`.`multi_type_number_unsigned` values (2, 200, 40000, 10000000, 3000000000, 600000000000, 200000000000000, 40000000000000000, 10000000000000000000,\n" +
            "  200, 40000, 10000000, 3000000000, 3000000000, 10000000000000000000, '2019-10-16 15:15:15.666614'\n" +
            ");";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            MAX_NUM,
            MID_NUM
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`multi_type_number_unsigned` set `bit2` = 222 where `id` = 1;",
            "update `drc4`.`multi_type_number_unsigned` set `bit2` = 222 where `id` = 2;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`multi_type_number_unsigned` where `id` = 1;",
            "delete from `drc4`.`multi_type_number_unsigned` where `id` = 2;"
    );

    private static final String SELECT_MULTI_TYPE_NUMBER_MAX = "select * from `drc4`.`multi_type_number_unsigned` where `bit1` = 0;";

    private static final String SELECT_MULTI_TYPE_NUMBER_MID = "select * from `drc4`.`multi_type_number_unsigned` where `bit1` = 200;";

    public MultiTypeNumberUnsignedPairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_MULTI_TYPE_NUMBER_MAX, SELECT_MULTI_TYPE_NUMBER_MID);
    }
}
