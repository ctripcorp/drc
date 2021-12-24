package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * 事务多个事件跨库
 * Created by mingdongli
 * 2019/10/9 下午11:55.
 */
public class MultiDBWriteInTransactionPairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static final List<String> MULTI_DB_INSERT = Lists.newArrayList(
            "insert into `drc1`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc1`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc1`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666661');",
            "insert into `drc2`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc2`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc2`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666662');",
            "insert into `drc3`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('1', null, '四四四', '2019-10-16 15:15:15.666663');",
            "insert into `drc3`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('2', null, '四四四', '2019-10-16 15:15:15.666663');",
            "insert into `drc3`.`insert1` (`one`, `three`, `four`, `datachange_lasttime`) values ('3', null, '四四四', '2019-10-16 15:15:15.666663');"
    );

    private static final List<String> MULTI_DB_UPDATE = Lists.newArrayList(
            "update `drc1`.`insert1` set `three` = '3',`four` = '4' where `one` = '1';",
            "update `drc1`.`insert1` set `three` = '3', `four` = '4' where `one` = '2';",
            "update `drc1`.`insert1` set `three` = '3', `four` = '4' where `one` = '3';",
            "update `drc2`.`insert1` set `three` = '3', `four` = '4' where `one` = '1';",
            "update `drc2`.`insert1` set `three` = '3', `four` = '4' where `one` = '2';",
            "update `drc2`.`insert1` set `three` = '3', `four` = '4' where `one` = '3';",
            "update `drc3`.`insert1` set `three` = '3', `four` = '4' where `one` = '1';",
            "update `drc3`.`insert1` set `three` = '3', `four` = '4' where `one` = '2';",
            "update `drc3`.`insert1` set `three` = '3', `four` = '4' where `one` = '3';"
    );

    private static final List<String> MULTI_DB_DELETE = Lists.newArrayList(
            "delete from `drc1`.`insert1` where `one` = '1';",
            "delete from `drc1`.`insert1` where `one` = '2';",
            "delete from `drc2`.`insert1` where `one` = '1';",
            "delete from `drc2`.`insert1` where `one` = '2';",
            "delete from `drc3`.`insert1` where `one` = '1';",
            "delete from `drc3`.`insert1` where `one` = '2';"
    );

    private static final String SELECT_DB_ONE = "select * from `drc1`.`insert1`;";

    private static final String SELECT_DB_TWO = "select * from `drc2`.`insert1`;";

    private static final String SELECT_DB_THREE = "select * from `drc3`.`insert1`;";

    public MultiDBWriteInTransactionPairCase() {
        initResource(MULTI_DB_INSERT, MULTI_DB_UPDATE, MULTI_DB_DELETE, SELECT_DB_ONE, SELECT_DB_TWO, SELECT_DB_THREE);
    }
}
