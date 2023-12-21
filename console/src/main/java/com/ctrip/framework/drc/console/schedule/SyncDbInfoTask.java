package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @ClassName SyncDbInfoTask
 * @Author haodongPan
 * @Date 2022/9/8 11:25
 * @Version: $
 */
@Component
@Order(2)
public class SyncDbInfoTask extends AbstractLeaderAwareMonitor implements NamedCallable<Boolean> {

    protected static final int INITIAL_DELAY = 5;

    protected static final int PERIOD = 60 * 4;

    protected static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    private final static int RETRY_TIME = 3;

    private Map<Long,String> buId2BuCodeMap = Maps.newHashMap();

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private OpenService openService;

    @Autowired
    private DbTblDao dbTblDao;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;


    @Override
    public void initialize() {
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
        setTimeUnit(TIME_UNIT);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        try {
            if (isRegionLeader && consoleConfig.isCenterRegion()) {
                logger.info("[[task=SyncDbInfoTask]] is leader");
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.schedule", "syncDBInfoFromCMSTask",
                        new RetryTask<>(this, RETRY_TIME)
                );
            } else {
                logger.info("[[task=SyncDbInfoTask]]not a leader do nothing");
            }
        } catch (Throwable t) {
            logger.info("[[task=SyncDbInfoTask]]error", t);
        }
    }

    @Override
    public Boolean call() throws Exception {
        String syncDbInfoSwitch = monitorTableSourceProvider.getSyncDbInfoSwitch();
        if (SWITCH_STATUS_ON.equalsIgnoreCase(syncDbInfoSwitch)) {
            openService.refreshBuInfoMap(buId2BuCodeMap);
            logger.info("[[task=SyncDbInfoTask]]sync all mha instance group");
            JsonArray dbArray = openService.getDbArray();
            if (dbArray.size() == 0) {
                return true;
            }
            syncDbInfo(dbArray);
        } else {
            logger.warn("[[task=SyncDbInfoTask]] is leader but switch is off");
        }
        return true;
    }


    @Override
    public void afterException(Throwable t) {
        logger.warn("[[task=SyncDbInfoTask]] task error once",t);
    }

    @Override
    public void afterFail() {
        NamedCallable.super.afterFail();
        logger.error("[[task=SyncDbInfoTask]] task fail after reTry time:{}",RETRY_TIME);
    }

    @Override
    public void afterSuccess(int retryTime) {
        logger.info("[[task=SyncDbInfoTask]] success after retryTime:{}",retryTime);
    }

    @VisibleForTesting
    protected void syncDbInfo(JsonArray dbArray) {
        try {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.schedule", "all_update_db_info",
                    () -> {
                        int len = dbArray.size();
                        logger.info("[[task=SyncDbInfoTask]] sync DbInfo from cms, count:{}", len);
                        if (len == 0) {
                            return;
                        }

                        //handle offline dbs
//                        List<String> dbWhiteList = monitorTableSourceProvider.getSyncDbWhitelist();
//                        List<String> remoteDbNameList = new ArrayList<>();
//                        for (JsonElement jsonElement : dbArray) {
//                            JsonObject jsonObject = jsonElement.getAsJsonObject();
//                            String name = jsonObject.get("db_name").getAsString().toLowerCase();
//                            remoteDbNameList.add(name);
//                        }


//                        List<DbTbl> dbInfosInDb = dbTblDao.queryAll();
//                        final List<DbTbl> excessDbInfos = dbInfosInDb.stream().filter(
//                                dbInfoInDb -> !remoteDbNameList.contains(dbInfoInDb.getDbName()) && !dbWhiteList.contains(dbInfoInDb.getDbName())).collect(Collectors.toList());

                        // update
                        int size = 100;
                        int pages = len / size + 1;
                        logger.info("[[task=SyncDbInfoTask]] start By {} batches", pages);
                        for (int i = 0; i < pages; i++) {
                            long start = System.currentTimeMillis();
                            logger.info("[[task=SyncDbInfoTask,batch={}]] start batch!", i);
                            try {
                                List<String> dbNames = new LinkedList<>();
                                List<DbTbl> inserts = new LinkedList<>();
                                List<DbTbl> updates = new LinkedList<>();
                                for (int j = i * size; j < i * size + size && j < len; j++) {
                                    JsonObject dbInfo = dbArray.get(j).getAsJsonObject();
                                    String dbName = dbInfo.get("db_name").getAsString();
                                    dbNames.add(dbName);
                                }

                                List<DbTbl> dbs = dbTblDao.queryByDbNames(dbNames);
                                for (int m = i * size; m < i * size + size && m < len; m++) {
                                    JsonObject dbInfo = dbArray.get(m).getAsJsonObject();
                                    DbTbl dbEntity = new DbTbl();
                                    setValue(dbEntity, dbInfo);
                                    boolean noSameDbInMetaDB = true;
                                    for (DbTbl dbTblInMetaDb : dbs) {
                                        if (dbTblInMetaDb.getDbName().equalsIgnoreCase(dbInfo.get("db_name").getAsString())) {
                                            dbEntity.setId(dbTblInMetaDb.getId());
                                            updates.add(dbEntity);
                                            noSameDbInMetaDB = false;
                                            break;
                                        }
                                    }
                                    if (noSameDbInMetaDB) {
                                        inserts.add(dbEntity);
                                    }
                                }
                                if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUpdateDbInfoSwitch())) {
                                    dbTblDao.update(updates);
                                    logger.info("[[task=SyncDbInfoTask,batch={}]] updateBatch execute,count:{}", i, updates.size());
                                }
                                dbTblDao.insert(inserts);
                                logger.info("[[task=SyncDbInfoTask,batch={}]] insertBatch execute,count:{}", i, inserts.size());
                                
                            } catch (Exception e) {
                                logger.error("[[task=SyncDbInfoTask,batch={}]] batch action fail", i, e);
                            } finally {
                                long end = System.currentTimeMillis();
                                logger.info("[[task=SyncDbInfoTask,batch={}]] this batch cost time:{}", i, end - start);
                            }
                        }
//                        if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUpdateDbInfoSwitch())) {
//                            dbTblDao.batchDelete(excessDbInfos);
//                            logger.info("[[task=SyncDbInfoTask]] delete all offline DbInfo done,delete_batch size:{}, excessDbInfos: {}", excessDbInfos.size(), excessDbInfos);
//                        }
//                        logger.info("[[task=SyncDbInfoTask]] delete all offline DbInfo done,delete_batch size:{}, excessDbInfos: {}", excessDbInfos.size(), excessDbInfos);
                    });
        } catch (Exception e) {
            logger.error("[[task=SyncDbInfoTask]] sync all DbInfo error", e);
        }
    }

    @VisibleForTesting
    protected void setValue(DbTbl target,JsonObject source) {
        target.setDbName(source.get("db_name").isJsonNull() ? null : source.get("db_name").getAsString());
        target.setBuName(source.get("organization_name").isJsonNull() ? null : source.get("organization_name").getAsString());
        target.setDbOwner(source.get("dbowners").isJsonNull() ? null : source.get("dbowners").getAsString().split(";")[0]);
        long organizationId = source.get("organization_id").getAsLong();
        String buCode = buId2BuCodeMap.get(organizationId);
        target.setBuCode(StringUtils.isEmpty(buCode) ? null : buCode);
    }
    


    @Override
    public void switchToLeader() throws Throwable {
        // do nothing
    }

    @Override
    public void switchToSlave() throws Throwable {
        // do nothing
    }
}
