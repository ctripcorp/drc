package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.monitor.Monitor;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.xpipe.api.monitor.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
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
@DependsOn({"dalServiceImpl", "drcMaintenanceServiceImpl"})
public class SyncMhaTask extends AbstractMonitor implements Monitor {

    public static final int INITIAL_DELAY = 1;

    public static final int PERIOD = 5;

    public static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

    @Autowired
    private DalServiceImpl dalService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Override
    public void scheduledTask() {
        try {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.console.syncMha", "syncFromDal", () -> {
                String syncMhaSwitch = monitorTableSourceProvider.getSyncMhaSwitch();
                if (SWITCH_STATUS_ON.equalsIgnoreCase(syncMhaSwitch)) {
                    logger.info("[[task=syncMhaTask]]sync all mha instance group");
                    Map<String, MhaInstanceGroupDto> mhaInstanceGroupMap = dalService.getMhaList(Foundation.server().getEnv());
                    updateAllMhaInstanceGroup(mhaInstanceGroupMap);
                }
            });
        } catch (Throwable t) {
            logger.info("[[task=SyncMhaTask]]cluster manager check error", t);
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
