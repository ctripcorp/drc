package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TimeConcern;
import com.ctrip.framework.drc.console.monitor.consistency.utils.DateUtils;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_DC_LOGGER;

public class StreamRangeQuerySingleExecution extends TimeRangeQuerySingleExecution implements NameAwareExecution, PagedExecution, TimeConcern {

    private DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MonitorTableSourceProvider monitorTableSourceProvider = MonitorTableSourceProvider.getInstance();

    public StreamRangeQuerySingleExecution(TableNameDescriptor tableDescriptor) {
        super(tableDescriptor);
    }

    // query two minutes
    public static final String QUERY_SQL = "select * from %s where `%s`<= '%s' and `%s`> '%s' order by %s";

    @Override
    protected String getQuerySql() {
        String table = getTable();
        String onUpdate = getOnUpdate();
        String key = getKey();
        int beginTimeOffsetSecond = monitorTableSourceProvider.getDataConsistencyCheckBeginTimeOffsetSecond();
        int endTimeOffsetSecond = monitorTableSourceProvider.getDataConsistencyCheckEndTimeOffsetSecond();
        Date endDate = DateUtils.NSecondsAgo(getBaseDate(), beginTimeOffsetSecond);
        String endDateString = sdf.format(endDate);
        Date startDate = DateUtils.NSecondsAgo(getBaseDate(), endTimeOffsetSecond);
        String startDateString = sdf.format(startDate);
        String sql = String.format(QUERY_SQL, table, onUpdate, endDateString, onUpdate, startDateString, key);
        CONSOLE_DC_LOGGER.debug("[[monitor=dataConsistency]] sql({})", sql);
        return sql;
    }
}
