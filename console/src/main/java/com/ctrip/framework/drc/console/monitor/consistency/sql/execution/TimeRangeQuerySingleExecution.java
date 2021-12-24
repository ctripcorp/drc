package com.ctrip.framework.drc.console.monitor.consistency.sql.execution;

import com.ctrip.framework.drc.console.monitor.consistency.sql.page.PagedExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TimeConcern;
import com.ctrip.framework.drc.console.monitor.consistency.utils.DateUtils;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by mingdongli
 * 2019/11/12 下午5:21.
 */
public class TimeRangeQuerySingleExecution extends AbstractNameAwareExecution implements NameAwareExecution, PagedExecution, TimeConcern {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    private DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//    public static final int MAX_PAGE = 24;  //700 ms to compare one page, QPS 80
//
//    public static final int BEGIN_TIME_OFFSET_SECOND = 60;
//
//    public static final int END_TIME_OFFSET_SECOND = 90;
//
//    public static final int TIME_INTERVAL_SECOND = END_TIME_OFFSET_SECOND - BEGIN_TIME_OFFSET_SECOND;

    public static final int MAX_PAGE = MonitorTableSourceProvider.getInstance().getDataConsistencyCheckMaxPage();

    public static final int BEGIN_TIME_OFFSET_SECOND = MonitorTableSourceProvider.getInstance().getDataConsistencyCheckBeginTimeOffsetSecond();

    public static final int END_TIME_OFFSET_SECOND = MonitorTableSourceProvider.getInstance().getDataConsistencyCheckEndTimeOffsetSecond();

    public static final int TIME_INTERVAL_SECOND = MonitorTableSourceProvider.getInstance().getDataConsistencyCheckTimeIntervalSecond();

    private boolean hasMore = false;

    private int nextPageNum = 1;

    private Date baseDate;

    // query two minutes
    public static final String QUERY_SQL = "select * from %s where `%s`<= '%s' and `%s`> '%s' order by %s limit %d offset %d";

    public TimeRangeQuerySingleExecution(TableNameDescriptor tableDescriptor) {
        super(tableDescriptor);
    }

    @Override
    protected String getQuerySql() {
        String table = getTable();
        String onUpdate = getOnUpdate();
        String key = getKey();
        Date endDate = DateUtils.NSecondsAgo(baseDate, BEGIN_TIME_OFFSET_SECOND);
        String endDateString = sdf.format(endDate);
        Date startDate = DateUtils.NSecondsAgo(baseDate, END_TIME_OFFSET_SECOND);
        String startDateString = sdf.format(startDate);
        return String.format(QUERY_SQL, table, onUpdate, endDateString, onUpdate, startDateString, key, LIMIT, (nextPageNum - 1) * LIMIT);
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    @Override
    public void setResultSize(int count) {
        hasMore = count == LIMIT ? true : false;
        if (hasMore) {
            nextPageNum++;
            if (nextPageNum > MAX_PAGE) {
                hasMore = false;
                nextPageNum = 1;
                logger.info("[Reset] nextPageNum for reaching max page num {}", MAX_PAGE);
            }
        } else {
            nextPageNum = 1;
            logger.info("[Reset] nextPageNum for result set count {}", count);
        }
    }

    @Override
    public void setTime(Date date) {
        this.baseDate = date;
    }

    public Date getBaseDate() {
        return baseDate;
    }
}
