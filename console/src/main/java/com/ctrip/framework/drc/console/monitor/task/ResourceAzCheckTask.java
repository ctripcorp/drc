package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceSameAzView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by shiruixin
 * 2024/7/5 16:22
 */
@Component
@Order(3)
public class ResourceAzCheckTask extends AbstractLeaderAwareMonitor  {
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ResourceService resourceService;
    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String RESOURCE_AZ_ERROR_NUM_MEASUREMENT = "fx.drc.resourceAZ.errNums";


    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(30);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void scheduledTask() throws Exception { //?
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getResourceAzCheckSwitch()) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] is leader, but switch is off");
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] is leader, going to check");
        check();
    }

    protected void check() throws Exception {
        ResourceSameAzView view = resourceService.checkResourceAz();
        Map<String, String> tag = new HashMap<>();
        tag.put("type","replicatorMha");
        reporter.resetReportCounter(tag, (long)view.getReplicatorMhaList().size() , RESOURCE_AZ_ERROR_NUM_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] replicatorMhaListCount: {}", view.getReplicatorMhaList().size());

        tag = new HashMap<>();
        tag.put("type","applierDb");
        reporter.resetReportCounter(tag, (long)view.getApplierDbList().size() , RESOURCE_AZ_ERROR_NUM_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] applierDbListCount: {}", view.getApplierDbList().size());

        tag = new HashMap<>();
        tag.put("type","applierMhaReplication");
        reporter.resetReportCounter(tag, (long)view.getApplierMhaReplicationList().size() , RESOURCE_AZ_ERROR_NUM_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] applierMhaReplicationListCount: {}", view.getApplierMhaReplicationList().size());

        tag = new HashMap<>();
        tag.put("type","messengerMha");
        reporter.resetReportCounter(tag, (long)view.getMessengerMhaList().size() , RESOURCE_AZ_ERROR_NUM_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ResourceAzCheckTask]] messengerMhaListCount: {}", view.getMessengerMhaList().size());

    }
}
