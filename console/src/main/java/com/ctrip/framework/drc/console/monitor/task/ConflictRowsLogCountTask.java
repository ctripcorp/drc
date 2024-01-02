package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogCountView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by dengquanliang
 * 2023/12/26 17:31
 */
@Component
@Order(3)
public class ConflictRowsLogCountTask extends AbstractLeaderAwareMonitor {

    @Autowired
    private ConflictLogService conflictLogService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String ROW_LOG_COUNT_MEASUREMENT = "row.log.count";
    private static final String ROW_LOG_DB_COUNT_MEASUREMENT = "row.log.db.count";
    private static final String ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT = "row.log.db.rollback.count";
    private static final String ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT = "row.log.count.query.time";
    private static long beginHandleTime = 0L;
    private static long endHandleTime = 0L;
    private static long endTimeOfDay = 0L;
    private static final int COUNT_SIZE = 50;
    private static int totalCount;
    private static int rollBackTotalCount;
    private static boolean nextDay = false;
    private static Map<String, ConflictRowsLogCount> tableCountMap;
    private static Map<String, ConflictRowsLogCount> rollBackCountMap;


    @Override
    public void initialize() {
        reset();
        setInitialDelay(1);
        setPeriod(5);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ConflictRowsLogCountTask]] is leader, going to check");
        try {
            checkCount();
            if (nextDay) {
                setEmpty();
            }
            beginHandleTime = endHandleTime;
            endHandleTime = getNextEndHandleTime(endHandleTime);
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=ConflictRowsLogCountTask]] fail, {}", e);
            reset();
        }
    }

    private void reset() {
        endHandleTime = System.currentTimeMillis();
        beginHandleTime = DateUtils.getStartTimeOfDay(endHandleTime);
        endTimeOfDay = DateUtils.getEndTimeOfDay(endHandleTime);
        setEmpty();
    }


    private void setEmpty() {
        totalCount = 0;
        rollBackTotalCount = 0;
        tableCountMap = new HashMap<>();
        rollBackCountMap = new HashMap<>();
    }

    private void refreshAndReport(ConflictRowsLogCountView rowsLogCountView) {
        if (rowsLogCountView.getTotalCount() != null) {
            totalCount += rowsLogCountView.getTotalCount();
            reportTotalCount();
        }

        if (rowsLogCountView.getRollBackTotalCount() != null) {
            rollBackTotalCount += rowsLogCountView.getRollBackTotalCount();
            reportRollBackCount();
        }

        List<ConflictRowsLogCount> dbCounts = rowsLogCountView.getDbCounts();
        if (dbCounts != null) {
            for (ConflictRowsLogCount dbCount : dbCounts) {
                String tableName = dbCount.getDbName() + "." + dbCount.getTableName();
                ConflictRowsLogCount rowsLogCount = tableCountMap.get(tableName);
                if (rowsLogCount == null) {
                    tableCountMap.put(tableName, dbCount);
                } else {
                    rowsLogCount.setCount(rowsLogCount.getCount() + dbCount.getCount());
                }
            }
            reportDbCount();
        }

        List<ConflictRowsLogCount> rollBackDbCounts = rowsLogCountView.getRollBackDbCounts();
        if (rollBackDbCounts != null) {
            for (ConflictRowsLogCount dbCount : rollBackDbCounts) {
                String tableName = dbCount.getDbName() + "." + dbCount.getTableName();
                ConflictRowsLogCount rowsLogCount = rollBackCountMap.get(tableName);
                if (rowsLogCount == null) {
                    rollBackCountMap.put(tableName, dbCount);
                } else {
                    rowsLogCount.setCount(rowsLogCount.getCount() + dbCount.getCount());
                }
            }
            reportRollBackDbCount();
        }
    }

    protected void checkCount() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ConflictRowsLogCountView rowsLogCountView = conflictLogService.getRowsLogCountView(beginHandleTime, endHandleTime);
        stopWatch.stop();
        long costTime = stopWatch.getTotalTimeMillis();
        Map<String, String> countTimeTags = new HashMap<>();
        countTimeTags.put("type", "queryTime");
        reporter.resetReportCounter(countTimeTags, costTime, ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("ConflictRowsLogCountTask query count cost: {}", costTime);

        refreshAndReport(rowsLogCountView);
    }

    private void reportTotalCount() {
        Map<String, String> rowLogCountTags = new HashMap<>();
        rowLogCountTags.put("type", "total");
        reporter.resetReportCounter(rowLogCountTags, Long.valueOf(totalCount), ROW_LOG_COUNT_MEASUREMENT);
    }

    private void reportRollBackCount() {
        Map<String, String> rollBackRowLogCountTags = new HashMap<>();
        rollBackRowLogCountTags.put("type", "rollBack");
        reporter.resetReportCounter(rollBackRowLogCountTags, Long.valueOf(rollBackTotalCount), ROW_LOG_COUNT_MEASUREMENT);
    }

    private void reportDbCount() {
        List<ConflictRowsLogCount> dbCounts = new ArrayList<>(tableCountMap.values());
        Collections.sort(dbCounts);
        report(dbCounts, ROW_LOG_DB_COUNT_MEASUREMENT);
    }

    private void reportRollBackDbCount() {
        List<ConflictRowsLogCount> rollBackDbCounts = new ArrayList<>(rollBackCountMap.values());
        Collections.sort(rollBackDbCounts);
        report(rollBackDbCounts, ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT);
    }

    private void report(List<ConflictRowsLogCount> counts, String measurement) {
        if (CollectionUtils.isEmpty(counts)) {
            return;
        }
        int size = Math.min(counts.size(), COUNT_SIZE);

        for (int i = 0; i < size; i++) {
            ConflictRowsLogCount count = counts.get(i);
            Map<String, String> tags = new HashMap<>();
            tags.put("dbName", count.getDbName());
            tags.put("tableName", count.getTableName());
            reporter.resetReportCounter(tags, Long.valueOf(count.getCount()), measurement);
        }
    }

    private long getNextEndHandleTime(long endHandleTime) {
        long currentTime = System.currentTimeMillis();
        long currentEndTimeOfDay = DateUtils.getEndTimeOfDay(currentTime);
        if (currentEndTimeOfDay == endTimeOfDay) {
            nextDay = false;
            return currentTime;
        } else {
            nextDay = true;
            long nextEndHandleTime = DateUtils.getEndTimeOfDay(endHandleTime) + 1L;
            endTimeOfDay = DateUtils.getEndTimeOfDay(nextEndHandleTime);
            return nextEndHandleTime;
        }
    }
}
