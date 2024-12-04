package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaAzView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherInfoDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by shiruixin
 * 2024/9/14 10:26
 */
@Component
@Order(3)
public class InstancesAzCheckTask extends AbstractLeaderAwareMonitor {
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String MHA_DC_STATUS_MEASUREMENT = "fx.drc.mhaInDcCount";

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(30);
        setTimeUnit(TimeUnit.MINUTES);
        super.initialize();
    }

    @Override
    public void switchToSlave() throws Throwable {
        super.switchToSlave();
        reporter.removeRegister(MHA_DC_STATUS_MEASUREMENT);
    }

    @Override
    public void scheduledTask() throws Exception {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getInstancesAzCheckSwitch()) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] is leader, but switch is off");
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] is leader, going to check");
        check();
    }

    protected void check() throws Exception {
        Map<String, String> tag = new HashMap<>();
        MhaAzView mhaAzView = resourceService.getAllInstanceAzInfo();
        for (Map.Entry<String, Set<String>> entry : mhaAzView.getAz2mhaName().entrySet()) {
            String dcName = entry.getKey();
            Set<String> mhaNameInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "mha");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)mhaNameInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} mha in dc {}", mhaNameInDc.size(), dcName);
        }

        for (Map.Entry<String, List<String>> entry : mhaAzView.getAz2DbInstance().entrySet()) {
            String dcName = entry.getKey();
            List<String> dbInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "db_instance");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)dbInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} db instance in dc {}", dbInDc.size(), dcName);
        }

        for (Map.Entry<String, List<String>> entry : mhaAzView.getAz2ReplicatorInstance().entrySet()) {
            String dcName = entry.getKey();
            List<String> replicatorInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "replicator");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)replicatorInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} replicator instance in dc {}", replicatorInDc.size(), dcName);
        }

        for (Map.Entry<String, List<ApplierInfoDto>> entry : mhaAzView.getAz2ApplierInstance().entrySet()) {
            String dcName = entry.getKey();
            List<? extends FetcherInfoDto> applierInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "applier");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)applierInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} applier in dc {}", applierInDc.size(), dcName);
        }

        for (Map.Entry<String, List<MessengerInfoDto>> entry : mhaAzView.getAz2MessengerInstance().entrySet()) {
            String dcName = entry.getKey();
            List<? extends FetcherInfoDto> messengerInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "messenger");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)messengerInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} messenger in dc {}", messengerInDc.size(), dcName);
        }

        for (Map.Entry<String, Set<String>> entry : mhaAzView.getAz2DrcDb().entrySet()) {
            String dcName = entry.getKey();
            Set<String> dcRelatedDbs = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "drc_db");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)dcRelatedDbs.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=InstancesAzCheckTask]] {} drc related dbs in dc {}", dcRelatedDbs.size(), dcName);
        }

    }
}
