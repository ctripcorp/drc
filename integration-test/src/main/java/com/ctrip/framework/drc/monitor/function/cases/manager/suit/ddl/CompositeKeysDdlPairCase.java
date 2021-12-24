package com.ctrip.framework.drc.monitor.function.cases.manager.suit.ddl;

/**
 * Created by jixinwang on 2020/12/20
 */
public class CompositeKeysDdlPairCase extends GenericSingleSideDdlPairCase {

    protected static final String QUERY = "select * from `generic_ddl`.`composite_keys`;";

    protected static final String FIX_INSERT = "insert into `generic_ddl`.`composite_keys` (`name`, `datachange_lasttime`) values ('value2333', '2010-10-16 10:10:10.000661');";

    protected static final String FIX_UPDATE = "update `generic_ddl`.`composite_keys` set `datachange_lasttime` = now() where name = 'value2333';";

    protected static final String FIX_DELETE = "delete from `generic_ddl`.`composite_keys` where name = 'value2333';";

    @Override
    protected String getSqlFile() {
        return "ddl/compositeKeys.sql";
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
