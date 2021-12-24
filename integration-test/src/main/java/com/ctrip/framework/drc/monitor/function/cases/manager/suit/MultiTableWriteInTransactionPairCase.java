package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/12 上午11:30.
 */
public class MultiTableWriteInTransactionPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static final List<String> MULTI_TABLE_INSERT = Lists.newArrayList(
            "insert into `drc4`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc4`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc4`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc4`.`insert2` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc4`.`insert2` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc4`.`insert2` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc4`.`insert3` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666663');",
            "insert into `drc4`.`insert3` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666663');",
            "insert into `drc4`.`insert3` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666663');"
    );

    private static final List<String> MULTI_TABLE_UPDATE = Lists.newArrayList(
            "update `drc4`.`insert1` set `three` = '3' where `one` = '1';",
            "update `drc4`.`insert1` set `three` = '3' where `one` = '2';",
            "update `drc4`.`insert1` set `three` = '3' where `one` = '3';",
            "update `drc4`.`insert2` set `three` = '3' where `one` = '1';",
            "update `drc4`.`insert2` set `three` = '3' where `one` = '2';",
            "update `drc4`.`insert2` set `three` = '3' where `one` = '3';",
            "update `drc4`.`insert3` set `three` = '3' where `one` = '1';",
            "update `drc4`.`insert3` set `three` = '3' where `one` = '2';",
            "update `drc4`.`insert3` set `three` = '3' where `one` = '3';"
    );

    private static final List<String> MULTI_TABLE_DELETE = Lists.newArrayList(
            "delete from `drc4`.`insert1` where `one` = '1'",
            "delete from `drc4`.`insert1` where `one` = '2'",
            "delete from `drc4`.`insert2` where `one` = '1'",
            "delete from `drc4`.`insert2` where `one` = '2'",
            "delete from `drc4`.`insert3` where `one` = '1'",
            "delete from `drc4`.`insert3` where `one` = '2'"
    );

    private static final String SELECT_TABLE_ONE = "select * from `drc4`.`insert1`;";

    private static final String SELECT_TABLE_TWO = "select * from `drc4`.`insert2`;";

    private static final String SELECT_TABLE_THREE = "select * from `drc4`.`insert3`;";

    public MultiTableWriteInTransactionPairCase() {
        initResource(MULTI_TABLE_INSERT, MULTI_TABLE_UPDATE, MULTI_TABLE_DELETE, SELECT_TABLE_ONE, SELECT_TABLE_TWO, SELECT_TABLE_THREE);
    }
}
