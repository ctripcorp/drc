package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.Monitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.foundation.Foundation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-27
 */
@Component
@Order(1)
@DependsOn({"dalServiceImpl"})
public class SyncMhaTask extends AbstractLeaderAwareMonitor implements Monitor {

    public static final int INITIAL_DELAY = 1;

    public static final int PERIOD = 5;

    public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    @Autowired private DalServiceImpl dalService;

    @Autowired private DbMetaCorrectService dbMetaCorrectService;

    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired private MhaTblV2Dao mhaTblV2Dao;
    
    private final static int RETRY_TIME = 10;
    
    @Override
    public void scheduledTask() {
        try {
            if (isRegionLeader) {
                logger.info("[[task=syncMhaTask]]is leader");
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.schedule", "syncFromDal", 
                    new RetryTask<>(new SyncMhaFromDalTask(), RETRY_TIME)
                );
            } else {
                logger.info("[[task=syncMhaTask]]not a leader do nothing");
            }
        } catch (Throwable t) {
            logger.info("[[task=syncMhaTask]] error", t);
        }

    }
    
    private class SyncMhaFromDalTask implements NamedCallable<Boolean> {
        
        @Override
        public Boolean call() throws Exception {
            String syncMhaSwitch = monitorTableSourceProvider.getSyncMhaSwitch();
            if (SWITCH_STATUS_ON.equalsIgnoreCase(syncMhaSwitch)) {
                logger.info("[[task=syncMhaTask]]sync all mha instance group");
                Map<String, MhaInstanceGroupDto> mhaInstanceGroupMap = dalService.getMhaList(Foundation.server().getEnv());
                updateAllMhaInstanceGroup(mhaInstanceGroupMap);
            } else {
                logger.warn("[[task=syncMhaTask]] is leader but switch is off");
            }
            return true;
        }
        

        @Override
        public void afterException(Throwable t) {
            logger.warn("[[task=syncMhaTask]] task error once",t);
        }

        @Override
        public void afterFail() {
            NamedCallable.super.afterFail();
            logger.error("[[task=syncMhaTask]] task fail after reTry time:{}",RETRY_TIME);
        }

        @Override
        public void afterSuccess(int retryTime) {
            logger.info("[[task=syncMhaTask]]sync all mha instance success after retryTime:{}",retryTime);
        }
    }

    
    protected void updateAllMhaInstanceGroup(Map<String, MhaInstanceGroupDto> mhaInstanceGroupsMap) throws Exception {
        MhaTblV2 condition = new MhaTblV2();
        condition.setDeleted(BooleanEnum.FALSE.getCode());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryBy(condition);

        boolean updateFail = false;
        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
            MhaInstanceGroupDto mhaInstanceGroupDto = mhaInstanceGroupsMap.get(mhaTblV2.getMhaName());
            try {
                if (null != mhaInstanceGroupDto) {
                    logger.info("[[task=syncMhaTask]] update mha {} instance", mhaInstanceGroupDto.getMhaName());
                    dbMetaCorrectService.mhaInstancesChange(mhaInstanceGroupDto, mhaTblV2);
                }
            } catch (Exception e) {
                logger.error("[[task=syncMhaTask]] update mha {} instance fail", mhaTblV2.getMhaName(), e);
                updateFail = true;
            }
            if (updateFail) {
                throw new IllegalArgumentException("syncMhaTask fail");
            }
        }
    }

    @Override
    public int getDefaultInitialDelay() {
        return INITIAL_DELAY;
    }

    @Override
    public int getDefaultPeriod() {
        return PERIOD;
    }

    @Override
    public TimeUnit getDefaultTimeUnit() {
        return TIME_UNIT;
    }
    
}
