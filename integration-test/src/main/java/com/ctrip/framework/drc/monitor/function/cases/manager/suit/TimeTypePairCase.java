package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/21 下午11:30.
 */
public class TimeTypePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    public static final String TIME_TYPE1 = "insert into `drc4`.`time_type` (`id`, `date`, `time`, `time6`, `datetime`, `datetime6`, `timestamp`, `timestamp6`, `year`, `year4`, `appid`, `datachange_lasttime`)values (\n" +
            "0,'2019-10-16 15:15:15.666666',\n" +
            "'15:15:15.666666',\n" +
            "'15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019',\n" +
            "'19',\n" +
            "2019,\n" +
            "'2019-10-16 15:15:15.666618'\n" +
            ");";

    public static final String TIME_TYPE2 = "insert into `drc4`.`time_type` (`id`, `date`, `time`, `time6`, `datetime`, `datetime6`, `timestamp`, `timestamp6`, `year`, `year4`, `appid`, `datachange_lasttime`)values (\n" +
            "0,'2019-10-16 15:15:15.666666',\n" +
            "'15:15:15.666666',\n" +
            "'15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019-10-16 15:15:15.666666',\n" +
            "'2019',\n" +
            "'19',\n" +
            "2020,\n" +
            "'2019-10-16 15:15:15.666618'\n" +
            ");";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            TIME_TYPE1,
            TIME_TYPE2
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`time_type` set `time` = '22:22:22.222222' where `appid` = 2019;"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`time_type` where `appid` = 2019;",
            "delete from `drc4`.`time_type` where `appid` = 2020;"
    );

    private static final String SELECT_TIME_TYPE = "select * from `drc4`.`time_type`;";

    public TimeTypePairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS, SELECT_TIME_TYPE);
    }
}
