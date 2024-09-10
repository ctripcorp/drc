package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaAzView;
import com.ctrip.framework.drc.console.vo.v2.MhaSyncView;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by shiruixin
 * 2024/7/15 15:50
 */
@Component
@Order(3)
public class MhaSyncStatusCheckTask extends AbstractLeaderAwareMonitor {
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Autowired
    private ResourceService resourceService;

    private Reporter reporter = DefaultReporterHolder.getInstance();
    private static final String MHA_SYNC_STATUS_MEASUREMENT = "fx.drc.mhaSyncCount";
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
        reporter.removeRegister(MHA_SYNC_STATUS_MEASUREMENT);
        reporter.removeRegister(MHA_DC_STATUS_MEASUREMENT);
    }

    @Override
    public void scheduledTask() throws Exception {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        if (!consoleConfig.getMhaSyncStatusCheckSwitch()) {
            CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] is leader, but switch is off");
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] is leader, going to check");
        check();
    }

    protected void check() throws Exception {
        MhaSyncView view = mhaReplicationServiceV2.mhaSyncCount();
        Map<String, String> tag = new HashMap<>();
        tag.put("type","mha_sync_num");
        reporter.resetReportCounter(tag, (long)view.getMhaSyncIds().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] mha_sync_num: {}", view.getMhaSyncIds().size());

        tag = new HashMap<>();
        tag.put("type","db_num");
        reporter.resetReportCounter(tag, (long)view.getDbNameSet().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] db_num: {}", view.getDbNameSet().size());

        tag = new HashMap<>();
        tag.put("type","db_sync_num");
        reporter.resetReportCounter(tag, (long)view.getDbSyncSet().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] db_sync_num: {}", view.getDbSyncSet().size());

        tag = new HashMap<>();
        tag.put("type","dalcluster_num");
        reporter.resetReportCounter(tag, (long)view.getDalClusterSet().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] dalcluster_num: {}", view.getDalClusterSet().size());

        tag = new HashMap<>();
        tag.put("type","otter_db");
        reporter.resetReportCounter(tag, (long)view.getDbOtterSet().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] otter_db: {}", view.getDbOtterSet().size());

        tag = new HashMap<>();
        tag.put("type","messenger_db");
        reporter.resetReportCounter(tag, (long)view.getDbMessengerSet().size(), MHA_SYNC_STATUS_MEASUREMENT);
        CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] messenger_db: {}", view.getDbMessengerSet().size());

        MhaAzView mhaAzView = resourceService.getMhaAzCount();
        for (Map.Entry<String, Set<String>> entry : mhaAzView.getAz2mhaName().entrySet()) {
            String dcName = entry.getKey();
            Set<String> mhaNameInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "mha");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)mhaNameInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] {} mha in dc {}", mhaNameInDc.size(), dcName);
        }

        for (Map.Entry<String, List<String>> entry : mhaAzView.getAz2DbInstance().entrySet()) {
            String dcName = entry.getKey();
            List<String> dbInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "db_instance");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)dbInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] {} db instance in dc {}", dbInDc.size(), dcName);
        }

        for (Map.Entry<String, List<String>> entry : mhaAzView.getAz2ReplicatorInstance().entrySet()) {
            String dcName = entry.getKey();
            List<String> replicatorInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "replicator");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)replicatorInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] {} replicator instance in dc {}", replicatorInDc.size(), dcName);
        }

        for (Map.Entry<String, List<String>> entry : mhaAzView.getAz2ApplierInstance().entrySet()) {
            String dcName = entry.getKey();
            List<String> applierInDc = entry.getValue();
            tag = new HashMap<>();
            tag.put("type", "applier");
            tag.put("dc",dcName);
            reporter.resetReportCounter(tag, (long)applierInDc.size(), MHA_DC_STATUS_MEASUREMENT);
            CONSOLE_MONITOR_LOGGER.info("[[monitor=MhaSyncStatusCheckTask]] {} applier in dc {}", applierInDc.size(), dcName);
        }
    }
}
