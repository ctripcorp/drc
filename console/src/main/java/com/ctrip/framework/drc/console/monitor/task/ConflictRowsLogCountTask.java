package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogCountView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(5);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ConflictRowsLogCountTask]] is slave, going to check");
        try {
            checkCount();
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=ConflictRowsLogCountTask]] fail, {}", e);
        }
    }

    protected void checkCount() throws Exception {
        ConflictRowsLogCountView rowsLogCountView = conflictLogService.getRowsLogCountView();
        Integer totalCount = rowsLogCountView.getTotalCount();
        Integer rollBackTotalCount = rowsLogCountView.getRollBackTotalCount();
        List<ConflictRowsLogCount> dbCounts = rowsLogCountView.getDbCounts();
        List<ConflictRowsLogCount> rollBackDbCounts = rowsLogCountView.getRollBackDbCounts();

        Map<String, String> countTags = new HashMap<>();
        if (totalCount != null) {
            countTags.put("type", "total");
            reporter.resetReportCounter(countTags, Long.valueOf(totalCount), ROW_LOG_COUNT_MEASUREMENT);
        }
        if (rollBackTotalCount != null) {
            countTags.put("type", "rollBack");
            reporter.resetReportCounter(countTags, Long.valueOf(rollBackTotalCount), ROW_LOG_COUNT_MEASUREMENT);
        }

        report(dbCounts, ROW_LOG_DB_COUNT_MEASUREMENT);
        report(rollBackDbCounts, ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT);
    }

    private void report(List<ConflictRowsLogCount> counts, String measurement) {
        if (CollectionUtils.isEmpty(counts)) {
            return;
        }
        for (ConflictRowsLogCount count : counts) {
            Map<String, String> tags = new HashMap<>();
            tags.put("dbName", count.getDbName());
            tags.put("tableName", count.getTableName());
            reporter.resetReportCounter(tags, Long.valueOf(count.getCount()), measurement);
        }
    }
}
