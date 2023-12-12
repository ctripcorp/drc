package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.log.ConflictDbBlackListTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.service.email.Email;
import com.ctrip.framework.drc.core.service.email.EmailResponse;
import com.ctrip.framework.drc.core.service.email.EmailService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallConflictCount;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

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
    
    private OPSApiService opsApiService = ApiContainer.getOPSApiServiceImpl();
    private EmailService emailService = ApiContainer.getEmailServiceImpl();
    private Map<String,Integer> tableAlarmCountHourlyMap = Maps.newHashMap();
    private long periodCount = 0L;

    
    @Override
    public void initialize() {
        setInitialDelay(30);
        setPeriod(60);
        setTimeUnit(TimeUnit.SECONDS);
    }

    @Override
    public void scheduledTask() {
        try {
            periodCount++;
            boolean isClearBlackListPeriod = periodCount % 60 == 0;
            if (isClearBlackListPeriod) periodCount = 0L;
            if (isRegionLeader && consoleConfig.isCenterRegion()) {  
                logger.info("[[task=ConflictAlarm]] leader,scheduledTask");
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.conflict", "checkConflict", this::checkConflict);
                if (isClearBlackListPeriod) {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.conflict", "clearDRCBlackList",
                            () -> clearBlackListAddedAutomatically(LogBlackListType.AUTO));
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.conflict", "clearDBABlackList",
                            () -> clearBlackListAddedAutomatically(LogBlackListType.DBA));
                    tableAlarmCountHourlyMap.clear();
                }
            } else {
                logger.info("[[task=ConflictAlarm]]not a leader do nothing");
            }
        } catch (Throwable t) {
            logger.error("[[task=ConflictAlarm]]error", t);
        }
    }
    

    protected void clearBlackListAddedAutomatically(LogBlackListType type) throws SQLException {
        boolean clearSwitch;
        int expirationHour;
        switch (type) {
            case AUTO:
                clearSwitch = consoleConfig.getCflBlackListAutoAddSwitch();
                expirationHour = domainConfig.getCflBlackListExpirationHour();
                break;
            case DBA:
                clearSwitch = consoleConfig.getDBACflBlackListClearSwitch();
                expirationHour = domainConfig.getDBACflBlackListExpirationHour();
                break;
            default:
                throw new IllegalArgumentException("unSupport type " + type);
        }
        if (clearSwitch) {
            List<ConflictDbBlackListTbl> conflictDbBlackListTbls = cflLogBlackListTblDao.queryByType(type.getCode());
            long current = System.currentTimeMillis();
            List<ConflictDbBlackListTbl> toDelete = Lists.newArrayList();
            for (ConflictDbBlackListTbl blackListTbl : conflictDbBlackListTbls) {
                if (current - blackListTbl.getDatachangeLasttime().getTime() >= TimeUnit.HOURS.toMillis(expirationHour)) {
                    toDelete.add(blackListTbl);
                    if (toDelete.size() >= 100) {
                        cflLogBlackListTblDao.batchDelete(toDelete);
                        toDelete.clear();
                    }
                }
            }
            if (!toDelete.isEmpty()) {
                cflLogBlackListTblDao.batchDelete(toDelete);
            }
        }
    }
    
    protected void checkConflict() throws IOException, SQLException {
        String hickwallApi = domainConfig.getTrafficFromHickWall();
        String opsAccessToken = domainConfig.getOpsAccessToken();
        if (EnvUtils.fat()) {
            hickwallApi = domainConfig.getTrafficFromHickWallFat();
            opsAccessToken = domainConfig.getOpsAccessTokenFat();
        }
        List<HickWallConflictCount> commitTrxCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, true, true, 1);
        List<HickWallConflictCount> rollbackTrxCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, true, false, 1);
        List<HickWallConflictCount> commitRowsCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, false, true, 1);
        List<HickWallConflictCount> rollbackRowsCounts = opsApiService.getConflictCount(hickwallApi, opsAccessToken, false, false, 1);
        checkConflictCountAndAlarm(commitTrxCounts,ConflictCountType.CONFLICT_COMMIT_TRX);
        checkConflictCountAndAlarm(rollbackTrxCounts,ConflictCountType.CONFLICT_ROLLBACK_TRX);
        checkConflictCountAndAlarm(commitRowsCounts,ConflictCountType.CONFLICT_COMMIT_ROW);
        checkConflictCountAndAlarm(rollbackRowsCounts,ConflictCountType.CONFLICT_ROLLBACK_ROW);
    }
    
    
    private void checkConflictCountAndAlarm(List<HickWallConflictCount> cflRowCounts,ConflictCountType type) throws SQLException{
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
                if (!domainConfig.getSendConflictAlarmEmailSwitch() || isTriggerAlarmTooManyTimes(db,table,type) ) {
                    continue;
                }
                Email email = generateEmail(db, table, srcMha, dstMha, srcRegion, dstRegion, type, count);
                EmailResponse emailResponse = emailService.sendEmail(email);
                if (emailResponse.isSuccess()) {
                    logger.info("[[task=ConflictAlarm]]send email success, db:{}, table:{}, count:{}", db, table, count);
                } else {
                    logger.error("[[task=ConflictAlarm]]send email failed, message:{},db:{}, table:{}, count:{}", emailResponse.getMessage(),db, table, count);
                }
            }
        }
    }
    
    private boolean isTriggerAlarmTooManyTimes(String db, String table,ConflictCountType type)  { // todo hdpan test 
        Integer count = tableAlarmCountHourlyMap.getOrDefault(db + "." +  table + "-" + type.name(),0);
        if (count >= domainConfig.getConflictAlarmTimesPerHour()) {
            return true;
        }
        tableAlarmCountHourlyMap.put(db + table,count + 1);
        return false;
    }
    
   
    private Email generateEmail(String db, String table, String srcMha, String dstMha, String srcRegion, String dstRegion, ConflictCountType type, Long count) throws SQLException{
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(Lists.newArrayList(db));
        if (dbTbls.isEmpty()) {
            logger.error("[[task=ConflictAlarm]]db:{} not found in drc", db);
            return null;
        }
        Email email = new Email();
        email.setSubject("DRC 数据同步冲突告警");
        email.setSender(domainConfig.getConflictAlarmSenderEmail());
        if (domainConfig.getSendDbOwnerConflictEmailToSwitch()) {
            email.addRecipient(dbTbls.get(0).getDbOwner() + "@trip.com");
            domainConfig.getConflictAlarmCCEmails().forEach(email::addCc);
        } else {
            domainConfig.getConflictAlarmCCEmails().forEach(email::addRecipient);
        }
        email.addContentKeyValue("冲突表", db + "." + table);
        email.addContentKeyValue("同步链路", srcMha + "(" + srcRegion + ")" + "=>" + dstMha + "(" + dstRegion + ")");
        email.addContentKeyValue("冲突类型", type.name());
        email.addContentKeyValue("1min冲突统计", count.toString());
        email.addContentKeyValue("监控", domainConfig.getDrcHickwallMonitorUrl() + "&var-mha=" + srcMha);
        email.addContentKeyValue("自助处理", domainConfig.getDrcConflictHandleUrl());
        return email;
    }
  
    
    private boolean isTriggerAlarm(Long count,ConflictCountType type) {
        switch (type) {
            case CONFLICT_COMMIT_TRX:
                return count >= domainConfig.getConflictCommitTrxThreshold();
            case CONFLICT_ROLLBACK_TRX:
                return count >= domainConfig.getConflictRollbackTrxThreshold();
            case CONFLICT_COMMIT_ROW:
                return count >= domainConfig.getConflictCommitRowThreshold(); 
            case CONFLICT_ROLLBACK_ROW:
                return count >= domainConfig.getConflictRollbackRowThreshold();
            default:
                return false;
        }
    }
    
    
}
