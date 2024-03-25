package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.ConflictRowsLogTblDao;
import com.ctrip.framework.drc.console.dao.log.ConflictTrxLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogCount;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictRowsLogTbl;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.enums.log.ConflictCountType;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.log.ConflictLogService;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogCountView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private DbTblDao dbTblDao;
    @Autowired
    private ConflictRowsLogTblDao conflictRowsLogTblDao;
    @Autowired
    private ConflictTrxLogTblDao conflictTrxLogTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private DomainConfig domainConfig;

    private EmailService emailService = ApiContainer.getEmailServiceImpl();
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
    private static boolean alarmIsSent = true;
    private static Map<String, ConflictRowsLogCount> tableCountMap;
    private static Map<String, ConflictRowsLogCount> rollBackCountMap;
    private static List<ConflictRowsLogCount> yesterdayTopLogTables;
    private static List<ConflictRowsLogCount> yesterdayTopRollbackLogTables;


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
        CONSOLE_MONITOR_LOGGER.info("local region: " + consoleConfig.getRegion());
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ConflictRowsLogCountTask]] is leader, going to check");
        try {
            checkCount();
            alarm();
            if (nextDay) {
                setEmpty();
                alarmIsSent = false;
            }
            beginHandleTime = endHandleTime;
            endHandleTime = getNextEndHandleTime(endHandleTime);
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=ConflictRowsLogCountTask]] fail, {}", e);
            reset();
        }
    }

    public void switchToLeader() {
        reset();
    }

    public void switchToSlave() {
        removeRegister();
    }

    private void reset() {
        endHandleTime = System.currentTimeMillis();
        beginHandleTime = DateUtils.getStartTimeOfDay(endHandleTime);
        endTimeOfDay = DateUtils.getEndTimeOfDay(endHandleTime);
        alarmIsSent = true;
        yesterdayTopLogTables = new ArrayList<>();
        yesterdayTopRollbackLogTables = new ArrayList<>();
        setEmpty();
    }

    private void removeRegister() {
        reporter.removeRegister(ROW_LOG_COUNT_MEASUREMENT);
        reporter.removeRegister(ROW_LOG_COUNT_QUERY_TIME_MEASUREMENT);
        reporter.removeRegister(ROW_LOG_DB_COUNT_MEASUREMENT);
        reporter.removeRegister(ROW_LOG_DB_COUNT_ROLLBACK_MEASUREMENT);
    }

    private void setEmpty() {
        totalCount = 0;
        rollBackTotalCount = 0;
        tableCountMap = new HashMap<>();
        rollBackCountMap = new HashMap<>();
        removeRegister();
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

    protected void alarm() {
        if (alarmIsSent || !domainConfig.getConflictAlarmSendEmailSwitch()) {
            return;
        }
        long curTime = System.currentTimeMillis();
        long sendTime = DateUtils.getTimeOfHour(domainConfig.getConflictAlarmSendTimeHour());
        if (curTime < sendTime) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ConflictRowsLogCountTask]] send email alarm");
        try {
            alarm(yesterdayTopLogTables, ConflictCountType.CONFLICT_ROW);
            alarm(yesterdayTopRollbackLogTables, ConflictCountType.CONFLICT_ROLLBACK_ROW);
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=ConflictRowsLogCountTask]] error, {}", e);
        }
        alarmIsSent = true;
    }

    private void alarm(List<ConflictRowsLogCount> logCounts, ConflictCountType type) throws SQLException {
        if (CollectionUtils.isEmpty(logCounts)) {
            CONSOLE_MONITOR_LOGGER.info("alarm switch is off or logCounts are empty");
            return;
        }

        List<Long> rowLogIds = logCounts.stream().map(ConflictRowsLogCount::getRowLogId).collect(Collectors.toList());
        List<Long> trxLogIds = logCounts.stream().map(ConflictRowsLogCount::getTrxLogId).collect(Collectors.toList());
        List<ConflictRowsLogTbl> rowsLogTbls = conflictRowsLogTblDao.queryByIds(rowLogIds);
        List<ConflictTrxLogTbl> trxLogTbls = conflictTrxLogTblDao.queryByIds(trxLogIds);
        Map<Long, ConflictRowsLogTbl> rowLogMap = rowsLogTbls.stream().collect(Collectors.toMap(ConflictRowsLogTbl::getId, Function.identity()));
        Map<Long, ConflictTrxLogTbl> trxLogMap = trxLogTbls.stream().collect(Collectors.toMap(ConflictTrxLogTbl::getId, Function.identity()));

        for (ConflictRowsLogCount logCount : logCounts) {
            ConflictRowsLogTbl rowLog = rowLogMap.get(logCount.getRowLogId());
            ConflictTrxLogTbl trxLog = trxLogMap.get(logCount.getTrxLogId());
            Email email = generateEmail(logCount, type, rowLog, trxLog);
            EmailResponse emailResponse = emailService.sendEmail(email);
            if (emailResponse.isSuccess()) {
                CONSOLE_MONITOR_LOGGER.info("[[task=ConflictSendAlarm]]send email success, logCount: {}", logCount);
            } else {
                CONSOLE_MONITOR_LOGGER.error("[[task=ConflictSendAlarm]]send email failed, message: {}, logCount: {}", emailResponse.getMessage(), logCount);
            }
        }
    }

    private Email generateEmail(ConflictRowsLogCount count, ConflictCountType type, ConflictRowsLogTbl rowLog, ConflictTrxLogTbl trxLog) throws SQLException {
        String dbName = count.getDbName();
        String tableName = count.getTableName();
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(Lists.newArrayList(dbName));
        if (CollectionUtils.isEmpty(dbTbls)) {
            CONSOLE_MONITOR_LOGGER.error("[[monitor=ConflictRowsLogCountTask]] db: {} not exist", dbName);
            return null;
        }
        DbTbl dbTbl = dbTbls.get(0);
        Email email = new Email();
        email.setSubject("DRC 数据同步冲突告警");
        email.setSender(domainConfig.getConflictAlarmSenderEmail());
        boolean inBlacklist = conflictLogService.isInBlackListWithCache(dbName, tableName);
        if (domainConfig.getConflictAlarmSendDBOwnerSwitch() && !inBlacklist) {
            email.addRecipient(dbTbl.getDbOwner() + "@trip.com");
            domainConfig.getConflictAlarmCCEmails().forEach(email::addCc);
            if (StringUtils.isNotBlank(dbTbl.getEmailGroup())) {
                email.addCc(dbTbl.getEmailGroup());
            }
        } else {
            domainConfig.getConflictAlarmCCEmails().forEach(email::addRecipient);
        }
        email.addContentKeyValue("冲突表", dbName + "." + tableName);
        email.addContentKeyValue("冲突类型", type.name());
        email.addContentKeyValue("昨日冲突总数", String.valueOf(count.getCount()));
        email.addContentKeyValue("同步链路", trxLog.getSrcMhaName() + "(" + rowLog.getSrcRegion() + ")" + "=>" + trxLog.getDstMhaName() + "(" + rowLog.getDstRegion() + ")");
        email.addContentKeyValue("监控", domainConfig.getConflictAlarmHickwallUrl() + "&var-mha=" + trxLog.getSrcMhaName());
        email.addContentKeyValue("冲突查询", domainConfig.getConflictAlarmDrcUrl());
        email.addContentKeyValue("冲突用户文档", domainConfig.getCflUserDocumentUrl());

        String dbFilter = dbName + "\\." + tableName;
        email.addContentKeyValue("加入黑名单", domainConfig.getCflAddBlacklistUrl() + "&dbFilter=" + dbFilter + "\n");
        return email;
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
        if (nextDay) {
            int size = Math.min(domainConfig.getConflictAlarmTopNum(), dbCounts.size());
            yesterdayTopLogTables = dbCounts.subList(0, size);
        }

        report(dbCounts, ROW_LOG_DB_COUNT_MEASUREMENT);
    }

    private void reportRollBackDbCount() {
        List<ConflictRowsLogCount> rollBackDbCounts = new ArrayList<>(rollBackCountMap.values());
        Collections.sort(rollBackDbCounts);
        if (nextDay) {
            int size = Math.min(domainConfig.getConflictAlarmRollbackTopNum(), rollBackDbCounts.size());
            yesterdayTopRollbackLogTables = rollBackDbCounts.subList(0, size);
        }

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

    //for test
    protected static void setNextDay() {
        nextDay = true;
        alarmIsSent = false;
    }
}
