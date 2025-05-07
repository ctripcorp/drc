package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by shiruixin
 * 2024/12/23 16:19
 */
@Component
@Order(3)
public class SyncOfflinedMhaTask extends AbstractLeaderAwareMonitor {
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(30);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() throws Exception {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getSyncOfflinedMhaSwitch()) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=SyncOfflinedMhaTask]] is leader, but switch is off");
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=SyncOfflinedMhaTask]] is leader, going to check");
        check();
    }

    protected void check() throws Exception {
        Map<String, MhaInstanceGroupDto> mhaInstanceGroupsMap = resourceService.getMhaInstanceGroupsInAllRegions();
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryAllExist();
        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
            MhaInstanceGroupDto mhaInstanceGroupDto = mhaInstanceGroupsMap.get(mhaTblV2.getMhaName());
            if (mhaInstanceGroupDto == null) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.offline.mha", mhaTblV2.getMhaName());
            }
        }
    }
}
