package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:15.
 */
public class MultiTypeNumberPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String MAX_NUM = "insert into `drc4`.`multi_type_number` values (1, 255, 65535, 16777215, 4294967295, 1099511627775, 281474976710655, 72057594037927935, 18446744073709551615,  127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807, '2019-10-16 15:15:15.666610');";

    public static final String MIN_NUM = "insert into `drc4`.`multi_type_number` values (2, 0, 0, 0, 0, 0, 0, 0, 0,\n" +
            "  -128, -32768, -8388608, -2147483648, -2147483648, -9223372036854775808, '2019-10-16 15:15:15.666611'\n" +
            ");";


    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            MAX_NUM,
            MIN_NUM
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`multi_type_number` set `bit2` = 222 where `id` = 1;",
            "update `drc4`.`multi_type_number` set `bit2` = 222 where `id` = 2;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`multi_type_number` where `id` = 1;",
            "delete from `drc4`.`multi_type_number` where `id` = 2;"
    );


    private static final String SELECT_MULTI_TYPE_NUMBER_MAX = "select * from `drc4`.`multi_type_number` where `bit1` = 255;";

    private static final String SELECT_MULTI_TYPE_NUMBER_MIN = "select * from `drc4`.`multi_type_number` where `bit1` = 0;";

    public MultiTypeNumberPairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_MULTI_TYPE_NUMBER_MAX, SELECT_MULTI_TYPE_NUMBER_MIN);
    }
}
