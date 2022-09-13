package com.ctrip.framework.drc.console.schedule;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
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

    protected static final int PERIOD = 60 * 12;

    protected static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    private final static int RETRY_TIME = 3;

    private Map<Long,String> buId2BuCodeMap = Maps.newHashMap();

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private OpenService openService;

    @Autowired
    private DbTblDao dbTblDao;


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
            if (isRegionLeader) {
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
        Transaction t = Cat.newTransaction("DRC.console.schedule", "all_update_db_info");
        try {
            int len = dbArray.size();
            t.addData("allDbCount", len);
            if (len == 0) {
                return;
            }

            //handle offline dbs
            List<String> remoteDbNameList = new ArrayList<>();
            for (JsonElement jsonElement : dbArray) {
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                String name = jsonObject.get("db_name").getAsString();
                remoteDbNameList.add(name);
            }

            List<DbTbl> dbInfosInDb = dbTblDao.queryAll();
            final List<DbTbl> excessDbInfos = dbInfosInDb.stream().filter(
                    dbInfoInDb -> !remoteDbNameList.contains(dbInfoInDb.getDbName())).collect(Collectors.toList());

            // update
            int size = 100;
            int pages = len / size + 1;
            for (int i = 0; i < pages; i++) {
                Transaction t2 = Cat.newTransaction("Console.Schedule.Service.Batch", "batch_update_db_info");
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
                        boolean status = true;
                        for (DbTbl db : dbs) {
                            if (db.getDbName().equals(dbInfo.get("db_name").getAsString())) {
                                dbEntity.setId(db.getId());
                                updates.add(dbEntity);
                                status = false;
                                break;
                            }
                        }
                        if (status) {
                            inserts.add(dbEntity);
                        }
                    }
                    if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUpdateDbInfoSwitch())) {
                        dbTblDao.update(updates);
                        t2.addData("updateBatch",updates.size());
                    }
                    dbTblDao.insert(inserts);
                    t2.addData("current_batch:", i);
                    t2.addData("all_batch:", pages);
                    t2.addData("insertBatch",inserts.size());
                    t2.setStatus(Message.SUCCESS);
                } catch (Exception e) {
                    logger.error("[[task=SyncDbInfoTask]] batch action error",e);
                    t2.setStatus(e);
                    Cat.logError(e);
                } finally {
                    t2.complete();
                }
            }
            if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getUpdateDbInfoSwitch())) {
                dbTblDao.batchDelete(excessDbInfos);
                t.addData("delete_batch",excessDbInfos.size());
            }
            t.setStatus(Message.SUCCESS);
        } catch (Exception e) {
            logger.error("[[task=SyncDbInfoTask]] sync all DbInfo error",e);
            t.setStatus(e);
            Cat.logError(e);
        } finally {
            logger.info("[[task=SyncDbInfoTask]] sync all DbInfo done");
            t.complete();
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