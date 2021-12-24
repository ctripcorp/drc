package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

/**
 * @Author limingdong
 * @create 2020/3/18
 */
public class GenericDdlPairCase extends AbstractDdlPairCase implements PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    protected static final String QUERY = "select * from `generic_ddl`.`generic_test`;";

    protected static final String FIX_INSERT = "insert into `generic_ddl`.`generic_test` (`name`, `datachange_lasttime`) values ('value2333', '2010-10-16 10:10:10.000661');";

    protected static final String FIX_UPDATE = "update `generic_ddl`.`generic_test` set `datachange_lasttime` = now() where name = 'value2333';";

    protected static final String FIX_DELETE = "delete from `generic_ddl`.`generic_test` where name = 'value2333';";

    public GenericDdlPairCase() {
        initResource();
    }

    @Override
    protected String getSqlFile() {
        return "ddl/generic-ddl.sql";
    }

    @Override
    protected String getQuerySql() {
        return QUERY;
    }

    @Override
    protected String getFixInsert() {
        return FIX_INSERT;
    }

    @Override
    protected String getFixUpdate() {
        return FIX_UPDATE;
    }

    @Override
    protected String getFixDelete() {
        return FIX_DELETE;
    }



}
