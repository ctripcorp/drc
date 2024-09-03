package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.enums.log.CflBlacklistType;
import com.ctrip.framework.drc.console.enums.log.ConflictCountType;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ConflictAlarm
 * @Author haodongPan
 * @Date 2023/11/22 15:18
 * @Version: $
 */
@Component
@Order(3)
public class ConflictLogManager extends AbstractLeaderAwareMonitor {
    
    @Autowired
    private ConflictLogService conflictLogService; 
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ConflictDbBlackListTblDao cflLogBlackListTblDao;
    @Autowired
    private DbBlacklistCache dbBlacklistCache;
    @Autowired
    private DbaApiService dbaApiService;

    private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(4, "ConflictLogManager"));

    private static final int INITIAL_DELAY = 30;
    private static final int PERIOD = 60;
    
    private OPSApiService opsApiService = ApiContainer.getOPSApiServiceImpl();
    private EmailService emailService = ApiContainer.getEmailServiceImpl();
    private TransactionMonitor catMonitor =  DefaultTransactionMonitorHolder.getInstance();
    
    // key: db\.table
    protected final Map<String,Integer> tableAlarmCountMap = Maps.newHashMap();
    protected final Map<String,Integer> tableAlarmCountHourlyMap = Maps.newHashMap();
    
    protected int minuteCount = 0;
    private boolean isOverOneHour = false;
    private boolean isOverOneDay = false;
    
    
    
    @Override
    public void initialize() {
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
        setTimeUnit(TimeUnit.SECONDS);
    }

    @Override
    public void scheduledTask() {
        try {
            periodCalculate();
            if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
                logger.info("[[task=ConflictLogManager]]not a leader do nothing");
                return;
            }
            logger.info("[[task=ConflictLogManager]] leader,scheduledTask");
            catMonitor.logTransaction("ConflictLogManager", "checkConflict", this::checkConflictCount);
            catMonitor.logTransaction("ConflictLogManager", "clearUserBlackList", this::clearBlacklist);
            if (!isOverOneHour) {
                return;
            }
            catMonitor.logTransaction("ConflictLogManager", "addAlarmHotspotTable", this::addAlarmHotspotTablesToBlacklist);
        } catch (Throwable t) {
            logger.error("[[task=ConflictAlarm]]error", t);
        }
    }

    private void periodCalculate() {
        minuteCount++;
        isOverOneHour = minuteCount % 60 == 0;
        isOverOneDay = minuteCount % (60*24) == 0;
        if (isOverOneDay) {
            minuteCount = 0;
        }
    }

    @Override
    public void switchToLeader() throws Throwable {
        // doNothing
    }

    @Override
    public void switchToSlave() throws Throwable {
        // doNothing
    }

    private void clearBlacklist() throws Exception {
        List<ConflictDbBlackListTbl> cflBlackLists = cflLogBlackListTblDao.queryAllExist();
        if (cflBlackLists == null || cflBlackLists.isEmpty()) {
            return;
        }

        Map<CflBlacklistType, List<ConflictDbBlackListTbl>> typeListMap = Maps.newHashMap();
        for (ConflictDbBlackListTbl cflBlackList : cflBlackLists) {
            CflBlacklistType type = CflBlacklistType.getByCode(cflBlackList.getType());
            List<ConflictDbBlackListTbl> list = typeListMap.getOrDefault(type,Lists.newArrayList());
            list.add(cflBlackList);
            typeListMap.put(type, list);
        }

        boolean everClear = false;
        for (Map.Entry<CflBlacklistType, List<ConflictDbBlackListTbl>> entry : typeListMap.entrySet()) {
            CflBlacklistType type = entry.getKey();
            List<ConflictDbBlackListTbl> list = entry.getValue();
            initBlackListWithOutExpirationTime(list, type);
            everClear |= clearExpiredBlackList(list, type);
        }
        if (everClear) {
            dbBlacklistCache.refresh(true);
        }
    }

    // for init data with no expirationTime
    private void initBlackListWithOutExpirationTime(List<ConflictDbBlackListTbl> blackLists, CflBlacklistType type) throws SQLException {
        for (ConflictDbBlackListTbl blackListTbl : blackLists) {
            if (blackListTbl.getExpirationTime() != null) {
                continue;
            }
            if (type == CflBlacklistType.USER) {
                continue; // skip user blacklist config without expirationTime
            }
            int expirationHour = domainConfig.getBlacklistExpirationHour(type);
            Timestamp expirationTime = new Timestamp(blackListTbl.getCreateTime().getTime() + (long) expirationHour * 60 * 60 * 1000);
            blackListTbl.setExpirationTime(expirationTime);
            logger.info("init expirationTime ,type={}, blacklist={},expirationTime={}", type, blackListTbl,expirationTime);
            cflLogBlackListTblDao.update(blackListTbl);
        }
    }

    private boolean clearExpiredBlackList(List<ConflictDbBlackListTbl> allBlackList, CflBlacklistType type) throws SQLException {
        List<ConflictDbBlackListTbl> toBeDelete = Lists.newArrayList();
        for (ConflictDbBlackListTbl cflBlackList : allBlackList) {
            Timestamp expirationTime = cflBlackList.getExpirationTime();
            if (expirationTime == null) {
                continue;
            }
            if (expirationTime.before(new Timestamp(System.currentTimeMillis()))) {
                logger.info("blacklist expire, type={}, blacklist={},expirationTime={}", type, cflBlackList,expirationTime);
                toBeDelete.add(cflBlackList);
            }
        }
        if (domainConfig.getBlacklistClearSwitch(type)) {
            deleteBlackListByBatch(toBeDelete);
            return toBeDelete.size() > 0;
        }
        return false;
    }

    private void deleteBlackListByBatch(List<ConflictDbBlackListTbl> toBeDelete) throws SQLException {
        if (toBeDelete.isEmpty()) {
            return;
        }
        // delete toBeDelete by batch limit 100 one times
        int size = toBeDelete.size();
        int batchCount = size / 100;
        int mod = size % 100;
        for (int i = 0; i < batchCount; i++) {
            List<ConflictDbBlackListTbl> subList = toBeDelete.subList(i * 100, (i + 1) * 100);
            cflLogBlackListTblDao.batchDelete(subList);
        }
        if (mod > 0) {
            List<ConflictDbBlackListTbl> subList = toBeDelete.subList(batchCount * 100, size);
            cflLogBlackListTblDao.batchDelete(subList);
        }
    }

    protected void addAlarmHotspotTablesToBlacklist() {
        for (Map.Entry<String, Integer> entry : tableAlarmCountHourlyMap.entrySet()) {
            String table = entry.getKey();
            Integer hourlyCount = entry.getValue();
            Integer totalCount = tableAlarmCountMap.getOrDefault(table, 0);
            tableAlarmCountMap.put(table, totalCount + hourlyCount);
        }
        tableAlarmCountHourlyMap.clear();

        for (Map.Entry<String, Integer> entry : tableAlarmCountMap.entrySet()) {
            String table = entry.getKey();
            Integer count = entry.getValue();
            if (count >= domainConfig.getBlacklistAlarmHotspotThreshold()) {
                logger.info("[[task=ConflictAlarm]]table:{} alarm too many times:{},add to blacklist", table, count);
                try {
                    conflictLogService.addDbBlacklist(table, CflBlacklistType.ALARM_HOTSPOT,null);
                } catch (Exception e) {
                    logger.error("[[task=ConflictAlarm]]{},add ALARM_HOTSPOT Blacklist error", table,e);
                }
            }
        }
        if (isOverOneDay) {
            tableAlarmCountMap.clear();
        }
    }
    
    protected void checkConflictCount() throws Exception {
        String hickwallApi = domainConfig.getTrafficFromHickWall();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        List<HickWallConflictCount> commitTrxCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, true, true, 1);
        List<HickWallConflictCount> rollbackTrxCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, true, false, 1);
        List<HickWallConflictCount> commitRowsCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, false, true, 1);
        List<HickWallConflictCount> rollbackRowsCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, false, false, 1);

        checkConflictCountAndAlarm(commitTrxCounts, ConflictCountType.CONFLICT_COMMIT_TRX);
        checkConflictCountAndAlarm(rollbackTrxCounts,ConflictCountType.CONFLICT_ROLLBACK_TRX);
        checkConflictCountAndAlarm(commitRowsCounts,ConflictCountType.CONFLICT_COMMIT_ROW);
        checkConflictCountAndAlarm(rollbackRowsCounts,ConflictCountType.CONFLICT_ROLLBACK_ROW);
    }


    private void asyncCheckConflictCountAndAlarm(List<HickWallConflictCount> cflRowCounts, ConflictCountType type) {
        executorService.submit(() -> {
            try {
                checkConflictCountAndAlarm(cflRowCounts, type);
            } catch (Exception e) {
                logger.error("checkConflictCountAndAlarm error", e);
            }
        });
    }
    
    private void checkConflictCountAndAlarm(List<HickWallConflictCount> cflRowCounts,ConflictCountType type) throws Exception{
        for (HickWallConflictCount cflTableRowCount : cflRowCounts) {
            Long count = cflTableRowCount.getCount();
            if (isTriggerAlarm(count,type)) {
                String db = cflTableRowCount.getDb();
                String table = cflTableRowCount.getTable();
                if (conflictLogService.isInBlackListWithCache(db, table)) {
                    continue;
                }
                String srcMha = cflTableRowCount.getSrcMha();
                String dstMha = cflTableRowCount.getDestMha();
                String srcRegion = mhaServiceV2.getRegion(srcMha);
                String dstRegion = mhaServiceV2.getRegion(dstMha);

                logger.warn("[[task=ConflictAlarm]]type:{}, db:{}, table:{}, count:{}",type.name(),db, table, count);
                if (isTriggerAlarmTooManyTimesAnHour(db,table) || !domainConfig.getConflictAlarmSendEmailSwitch()) {
                    continue;
                }
                long currentTimeMillis = System.currentTimeMillis();
                long pastTime = currentTimeMillis - 1000 * 60 * 60 * 24 * 7; // one week
                boolean everUserTraffic = dbaApiService.everUserTraffic(dstRegion, db, table, pastTime, currentTimeMillis, false);
                Email email = generateEmail(everUserTraffic,db, table, srcMha, dstMha, srcRegion, dstRegion, type, count);
                if (!everUserTraffic) {
                    conflictLogService.addDbBlacklist(db + "\\." + table, CflBlacklistType.NO_USER_TRAFFIC,null);
                }
                EmailResponse emailResponse = emailService.sendEmail(email);
                if (emailResponse.isSuccess()) {
                    logger.info("[[task=ConflictAlarm]]send email success, db:{}, table:{}, count:{}", db, table, count);
                } else {
                    logger.error("[[task=ConflictAlarm]]send email failed, message:{},db:{}, table:{}, count:{}", emailResponse.getMessage(),db, table, count);
                }
            }
        }
    }
    
    private boolean isTriggerAlarmTooManyTimesAnHour(String db, String table)  {
        String key = db + "\\." +  table;
        Integer count = tableAlarmCountHourlyMap.getOrDefault(key,0);
        tableAlarmCountHourlyMap.put(key,count + 1);
        return count >= domainConfig.getConflictAlarmLimitPerHour();
    }
    
   
    private Email generateEmail(boolean everUserTraffic,String db, String table, String srcMha, String dstMha,
            String srcRegion, String dstRegion, ConflictCountType type, Long count) throws SQLException{
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(Lists.newArrayList(db));
        if (dbTbls.isEmpty()) {
            logger.error("[[task=ConflictAlarm]]db:{} not found in drc", db);
            return null;
        }
        DbTbl dbTbl = dbTbls.get(0);
        Email email = new Email();
        email.setSubject("DRC 数据同步冲突告警");
        email.setSender(domainConfig.getConflictAlarmSenderEmail());
        if (domainConfig.getConflictAlarmSendDBOwnerSwitch() && (everUserTraffic || type.isRollback())) {
            email.addRecipient(dbTbls.get(0).getDbOwner() + "@trip.com");
            domainConfig.getConflictAlarmCCEmails().forEach(email::addCc);
            if (StringUtils.isNotBlank(dbTbl.getEmailGroup())) {
                email.addCc(dbTbl.getEmailGroup());
            }
        } else {
            domainConfig.getConflictAlarmCCEmails().forEach(email::addRecipient);
        }
        email.addContentKeyValue("冲突表", db + "." + table);
        email.addContentKeyValue("同步链路", srcMha + "(" + srcRegion + ")" + "=>" + dstMha + "(" + dstRegion + ")");
        email.addContentKeyValue("冲突类型", type.name());
        email.addContentKeyValue("1min冲突统计", count.toString());
        email.addContentKeyValue("监控", domainConfig.getConflictAlarmHickwallUrl() + "&var-mha=" + srcMha);
        email.addContentKeyValue("冲突查询", domainConfig.getConflictAlarmDrcUrl());
        email.addContentKeyValue("冲突用户文档", domainConfig.getCflUserDocumentUrl());
        email.addContentKeyValue("目标端有无用户流量？若无则自动加入黑名单" ,everUserTraffic ? "有" : "无");
        String dbFilter = db + "\\." + table;
        email.addContentKeyValue("手动加入黑名单", domainConfig.getCflAddBlacklistUrl() + "&dbFilter=" + dbFilter + "\n");
        return email;
    }
  
    
    private boolean isTriggerAlarm(Long count,ConflictCountType type) {
        switch (type) {
            case CONFLICT_COMMIT_TRX:
                return count >= domainConfig.getConflictAlarmThresholdCommitTrx();
            case CONFLICT_ROLLBACK_TRX:
                return count >= domainConfig.getConflictAlarmThresholdRollbackTrx();
            case CONFLICT_COMMIT_ROW:
                return count >= domainConfig.getConflictAlarmThresholdCommitRow(); 
            case CONFLICT_ROLLBACK_ROW:
                return count >= domainConfig.getConflictAlarmThresholdRollbackRow();
            default:
                return false;
        }
    }
    
}
