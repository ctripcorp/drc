package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TimeConcern;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataTimeRangeQuerySingleExecution extends TimeRangeQuerySingleExecution implements NameAwareExecution, PagedExecution, TimeConcern {
    private String startTime;
    private String endTime;
    private static final String QUERY_SQL = "select * from %s where `%s`<= '%s' and `%s`>= '%s' order by %s desc;";
    private static final String QUERY_BEFORE_END_TIME_SQL = "select * from %s where `%s`<= '%s' order by %s desc;";

    public FullDataTimeRangeQuerySingleExecution(TableNameDescriptor tableDescriptor, String startTime, String endTime) {
        super(tableDescriptor);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    protected String getQuerySql() {

        String table = getTable();
        String onUpdate = getOnUpdate();
        String key = getKey();

        if (startTime == null) {
            return String.format(QUERY_BEFORE_END_TIME_SQL, table, onUpdate, endTime, onUpdate);
        }
        return String.format(QUERY_SQL, table, onUpdate, endTime, onUpdate, startTime, onUpdate);
    }
}
