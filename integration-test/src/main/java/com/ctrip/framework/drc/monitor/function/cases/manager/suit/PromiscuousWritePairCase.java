package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/12 下午1:51.
 */
public class PromiscuousWritePairCase extends AbstractMultiWriteInTransactionPairCase implements PairCase<ReadWriteSqlOperator, ReadSqlOperator<ReadResource>> {

    private static final String INSERT_STATEMENT = "insert into `drc4`.`component` (`charlt256`, `chareq256`, `varcharlt256`, `varchareq256`, `datachange_lasttime`) values\n" +
            "('中国1', '愚かな1', 'присоска1', 'abc1', '2019-10-16 15:15:15.666667'),\n" +
            "('中国2', '愚かな2', 'присоска2', 'abc2', '2019-10-16 15:15:15.666668'),\n" +
            "('中国3', '愚かな3', 'присоска3', 'abc3', '2019-10-16 15:15:15.666669'),\n" +
            "('中国4', '愚かな4', 'присоска4', 'abc4', '2019-10-16 15:15:15.666670');";

    private static final List<String> INSERT_STATEMENTS = Lists.newArrayList(
            INSERT_STATEMENT
    );

    private static final List<String> UPDATE_STATEMENTS= Lists.newArrayList(
            "update `drc4`.`component` set `charlt256`= '中国111' where `chareq256` = '愚かな1';",
            "update `drc4`.`component` set `charlt256`= '中国222' where `chareq256` = '愚かな2';",
            "update `drc4`.`component` set `charlt256`= '中国333' where `chareq256` = '愚かな3';"
    );

    private static final List<String> DELETE_STATEMENTS = Lists.newArrayList(
            "delete from `drc4`.`component` where `varchareq256` = 'abc1';",
            "delete from `drc4`.`component` where `varchareq256` = 'abc2';",
            "delete from `drc4`.`component` where `varchareq256` = 'abc4';"
    );

    private static final String SELECT_COMPONENT_CHARLT256 = "select * from `drc4`.`component` where `charlt256` = '中国3';";

    private static final String SELECT_COMPONENT_CHAREQ256 = "select * from `drc4`.`component` where `chareq256` = '愚かな3';";

    private static final String SELECT_COMPONENT_VARCHARLT256 = "select * from `drc4`.`component` where `varcharlt256` = 'присоска3';";

    private static final String SELECT_COMPONENT_VARCHAREQ256 = "select * from `drc4`.`component` where `varchareq256` = 'abc3';";

    public PromiscuousWritePairCase() {
        initResource(INSERT_STATEMENTS, UPDATE_STATEMENTS, DELETE_STATEMENTS,
                SELECT_COMPONENT_CHARLT256, SELECT_COMPONENT_CHAREQ256, SELECT_COMPONENT_VARCHARLT256, SELECT_COMPONENT_VARCHAREQ256);
    }
}
