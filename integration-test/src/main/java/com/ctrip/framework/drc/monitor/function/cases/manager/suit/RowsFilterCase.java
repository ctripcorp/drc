package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by jixinwang on 2022/5/30
 */
public class RowsFilterCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    // uids starting with trip can be sent to the peer
    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            "insert into `drc4`.`row_filter` (`uid`, `charlt256`, `chareq256`, `varcharlt256`, `varchargt256`, `datachange_lasttime`, `drc_id_int`, `addcol`, `addcol1`, `addcol2`, `drc_char_test_2`, `drc_tinyint_test_2`, `drc_bigint_test`, `drc_integer_test`, `drc_mediumint_test`, `drc_time6_test`, `drc_datetime3_test`, `drc_year_test`, `hourly_rate_3`, `drc_numeric10_4_test`, `drc_float_test`, `drc_double_test`, `drc_double10_4_test`, `drc_real_test`, `drc_real10_4_test`) values\n" +
                    "('trip2333', '中国1', '愚かな1', 'присоска1', 'abcdefg', NOW(), 123, 'addcol1', 'addcol2', 'addcol3', 'charvalue', 11, 22, 33, 44, '02:12:22', '2020-04-03 11:11:03', '2008', 1.04, 10.0, 13, 1234, 13.1212, 345, 12.3), \n" +
                    "('_TSHK7hhh23t57wl', '中国1', '愚かな1', 'присоска1', 'abcdefg', NOW(), 123, 'addcol1', 'addcol2', 'addcol3', 'charvalue', 11, 22, 33, 44, '02:12:22', '2020-04-03 11:11:03', '2008', 1.04, 10.0, 13, 1234, 13.1212, 345, 12.3);"
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`row_filter` set `charlt256` = '中国2022' where `id` > 0;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`row_filter` where `id` > 0;"
    );

    private static final String SELECT_TIME_TYPE = "select * from `drc4`.`row_filter` where `uid` = 'trip2333';";

    public RowsFilterCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_TIME_TYPE);
    }
}
