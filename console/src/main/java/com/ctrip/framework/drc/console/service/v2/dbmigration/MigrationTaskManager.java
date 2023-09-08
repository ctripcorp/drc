package com.ctrip.framework.drc.console.service.v2.dbmigration;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dao.v2.MigrationTaskTblDao;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.core.monitor.reporter.CatEventMonitor;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName MigrationTaskHelper
 * @Author haodongPan
 * @Date 2023/9/4 14:31
 * @Version: $
 */
@Component
public class MigrationTaskManager extends AbstractLeaderAwareMonitor {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationTaskManager.class);

    @Autowired
    private MigrationTaskTblDao migrationTaskTblDao;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Override
    public void initialize() {
        setInitialDelay(60);
        setPeriod(5);
        setTimeUnit(TimeUnit.SECONDS);
    }

    @Override
    public void scheduledTask() throws Throwable {
        try {
//            if (isRegionLeader && consoleConfig.isCenterRegion()) {
                logger.info("[[tag=MigrationTaskManager]] start to check tasks in pre-starting");
                List<MigrationTaskTbl> tasksInPreStarting = migrationTaskTblDao.queryByStatus(MigrationStatusEnum.PRE_STARTING.getStatus());
                tasksPreStartCheck(tasksInPreStarting);
//            } else {
//                logger.info("[[migrate=preStartCheck]] not leader, do nothing");
//            }
        } catch (Exception e) {
            logger.error("[[migrate=preStartCheck]] error when check tasks in pre-starting", e);
        }
    }
    
    private void tasksPreStartCheck(List<MigrationTaskTbl> tasks) throws Exception {
        if (CollectionUtils.isEmpty(tasks)) {
            logger.info("[[migrate=preStartCheck]] no task in pre-starting, do nothing");
            return;
        }
        Map<String, Long> mha2ReplicatorSlaveDelay = mhaServiceV2.getMhaReplicatorSlaveDelay(
                tasks.stream().map(MigrationTaskTbl::getNewMha).collect(Collectors.toList()));
        List<MigrationTaskTbl> toBeUpdate = Lists.newArrayList();
        for (MigrationTaskTbl task : tasks) {
            Long delay = mha2ReplicatorSlaveDelay.getOrDefault(task.getNewMha(), null);
            if (delay == null) {
                logger.warn("[[migrate=preStartCheck,taskId={}]] mha replicator slave delay {} not found, skip", task.getId(),task.getNewMha());
            } else {
                if (delay > TimeUnit.SECONDS.toMillis(10)) {
                    logger.info("[[migrate=preStartCheck,taskId={}]] mha replicator slave delay {} > 10s, skip", task.getId(),task.getNewMha());
                } else {
                    logger.info("[[migrate=preStartCheck,taskId={}]] mha replicator slave delay {} <= 10s, preStarted", task.getId(),task.getNewMha());
                    task.setStatus(MigrationStatusEnum.PRE_STARTED.getStatus());
                    toBeUpdate.add(task);
                }
            }
        }
        if (!CollectionUtils.isEmpty(toBeUpdate)) {
            int[] ints = migrationTaskTblDao.batchUpdate(toBeUpdate);
            logger.info("[[migrate=preStartCheck]] change to preStarted excepted:{},actual:{}",toBeUpdate.size(), Arrays.stream(ints).sum());
        }
        CatEventMonitor.DEFAULT.logEvent("DRC.DB.MigrationTask", "preStarting",tasks.size()-toBeUpdate.size());
    }
    
    

}
