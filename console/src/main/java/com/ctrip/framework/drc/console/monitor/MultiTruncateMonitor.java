package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DdlHistoryTblDao;
import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Maps;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName MultiTruncateMonitor
 * @Author haodongPan
 * @Date 2023/12/18 17:05
 * @Version: $
 */
@Order(3)
@Component
public class MultiTruncateMonitor extends  AbstractLeaderAwareMonitor {
    private static final String MULTI_TRUNCATE_LOSS_MEASUREMENT  = "fx.drc.multi.truncate.loss";
    

    @Autowired
    private DdlHistoryTblDao ddlHistoryTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;
    
    private Reporter reporter = DefaultReporterHolder.getInstance();
    
    


    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(30);
        setTimeUnit(TimeUnit.MINUTES);
    }


    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        try {
            long HOUR_MILLIS = 1000 * 60 * 60;
            Timestamp timestamp = new Timestamp(System.currentTimeMillis() - HOUR_MILLIS);
            List<DdlHistoryTbl> ddlHistories = ddlHistoryTblDao.queryByStartCreateTime(timestamp);
            // ddlHistories group by schemaName & tableName
            Map<String, Map<String, List<DdlHistoryTbl>>> dbTableTruncateListMap = ddlHistories.stream()
                    .collect(Collectors.groupingBy(DdlHistoryTbl::getSchemaName, Collectors.groupingBy(DdlHistoryTbl::getTableName)));
            for (Map.Entry<String, Map<String, List<DdlHistoryTbl>>> dbTableTruncateListEntry : dbTableTruncateListMap.entrySet()) {
                String db = dbTableTruncateListEntry.getKey();
                for (Map.Entry<String, List<DdlHistoryTbl>> tableTruncateListEntry : dbTableTruncateListEntry.getValue().entrySet()) {
                    String table = tableTruncateListEntry.getKey();
                    List<DdlHistoryTbl> truncateList = tableTruncateListEntry.getValue();
                    List<MhaTblV2> relatedMha = mhaDbReplicationService.getReplicationRelatedMha(db, table);
                    if (relatedMha.size() == 0) {
                        continue;
                    }
                    if (relatedMha.size() == truncateList.size()) {
                        logger.info("[[task=MultiTruncateMonitor]] consistent size:{}, db:{}, table:{}", truncateList.size(), db, table);
                        reporter.reportResetCounter(getReportTags(db,table,""),0L,MULTI_TRUNCATE_LOSS_MEASUREMENT);
                    } else if (relatedMha.size() > truncateList.size()) {
                        // inconsistent
                        Set<Long> truncatedMhaId = truncateList.stream().map(DdlHistoryTbl::getMhaId).collect(Collectors.toSet());
                        for (MhaTblV2 mhaTblV2 : relatedMha) {
                            if (!truncatedMhaId.contains(mhaTblV2.getId())) {
                                Timestamp createTime = truncateList.get(0).getCreateTime();
                                Timestamp before = new Timestamp(createTime.getTime() - HOUR_MILLIS);
                                Timestamp after = new Timestamp(createTime.getTime() + HOUR_MILLIS);
                                List<DdlHistoryTbl> ddlHistoryTbls = ddlHistoryTblDao.queryByDbAndTime(db, table, before, after);
                                boolean truncateExecuted = !CollectionUtils.isEmpty(ddlHistoryTbls) 
                                        && ddlHistoryTbls.stream().anyMatch(ddl -> ddl.getMhaId().equals(mhaTblV2.getId()));
                                if (truncateExecuted) {
                                    continue;
                                }
                                logger.warn("[[task=MultiTruncateMonitor]] loss truncate db:{}, table:{}, mha:{}", db, table, mhaTblV2.getMhaName());
                                reporter.reportResetCounter(getReportTags(db,table,mhaTblV2.getMhaName()),1L,MULTI_TRUNCATE_LOSS_MEASUREMENT);
                            }
                        }
                    } else {
                        reporter.reportResetCounter(getReportTags(db,table,""),-1L,MULTI_TRUNCATE_LOSS_MEASUREMENT);
                        logger.error("[[task=MultiTruncateMonitor]] exception, relatedMha:{} < truncateList:{}, db:{}, table:{}", 
                                relatedMha.size(), truncateList.size(), db, table);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("[[task=MultiTruncateMonitor]]error", t);
        }
    }
    
    
    public Map<String,String> getReportTags(String db, String table,String mha){
        Map<String,String> tags = Maps.newHashMap();
        tags.put("db",db);
        tags.put("table",table);
        tags.put("mha",mha);
        return tags;
    }


}
