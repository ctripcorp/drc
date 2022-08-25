package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.Monitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.DalPojo;
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
@DependsOn({"dalServiceImpl", "drcMaintenanceServiceImpl"})
public class SyncMhaTask extends AbstractLeaderAwareMonitor implements Monitor {

    public static final int INITIAL_DELAY = 1;

    public static final int PERIOD = 5;

    public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    private final static int RETRY_TIME = 10;
    
    @Override
    public void scheduledTask() {
        try {
            if (isRegionLeader) {
                logger.info("[[task=syncMhaTask]]is leader");
                DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.syncMha", "syncFromDal", 
                    new RetryTask<>(new SyncMhaFromDalTask(), RETRY_TIME)
                );
            } else {
                logger.info("[[task=syncMhaTask]]not a leader do nothing");
            }
        } catch (Throwable t) {
            logger.info("[[task=syncMhaTask]]cluster manager check error", t);
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
            logger.error("[[task=syncMhaTask]] task fail after reTry time:{}",RETRY_TIME);
        }

        @Override
        public void afterSuccess(int retryTime) {
            logger.info("[[task=syncMhaTask]]sync all mha instance success after retryTime:{}",retryTime);
        }
    }

    protected void updateAllMhaInstanceGroup(Map<String, MhaInstanceGroupDto> mhaInstanceGroupsMap) throws Exception {
        List<DalPojo> allPojos = TableEnum.MHA_TABLE.getAllPojos();
        for (DalPojo pojo : allPojos) {
            MhaTbl mhaTbl = (MhaTbl) pojo;
            MhaInstanceGroupDto mhaInstanceGroupDto = mhaInstanceGroupsMap.get(mhaTbl.getMhaName());
            if (null != mhaInstanceGroupDto) {
                logger.info("[[task=syncMhaTask]] update mha {} instance", mhaInstanceGroupDto.getMhaName());
                drcMaintenanceService.mhaInstancesChange(mhaInstanceGroupDto, mhaTbl);
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
