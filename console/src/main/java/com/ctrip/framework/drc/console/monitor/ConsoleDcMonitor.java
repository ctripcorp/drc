package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.ha.ConsoleDcLeaderElector;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.xpipe.zk.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONSOLE_MONITOR_LOGGER;

/**
 * Created by dengquanliang
 * 2025/3/4 10:56
 */
@Order(0)
@Component()
public class ConsoleDcMonitor extends AbstractLeaderAwareMonitor {

    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ZkClient zkClient;

    @Override
    public void initialize() {
        setInitialDelay(1);
        setPeriod(60);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        if (!isRegionLeader || !consoleConfig.isCenterRegion()) {
            return;
        }
        CONSOLE_MONITOR_LOGGER.info("[[monitor=ConsoleDcMonitor]] is leader, going to check");
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dc : dcsInLocalRegion) {
            String path = String.format(ConsoleDcLeaderElector.CONSOLE_DC_LEADER_ELECTOR_PATH, dc);
            List<String> allDcServers = getAllDcServers(path);
            if (CollectionUtils.isEmpty(allDcServers)) {
                DefaultEventMonitorHolder.getInstance().logEvent("Console.DC.EmptyMachine", dc);
            }
        }

    }

    private List<String> getAllDcServers(String path) {
        CuratorFramework curatorFramework = zkClient.get();
        List<String> children = null;
        List<String> result = new LinkedList<>();
        try {
            children = curatorFramework.getChildren().forPath(path);
            children = LockInternals.getSortedChildren("latch-", sorter, children);

            for (String child : children) {
                String currentPath = path + "/" + child;
                result.add(new String(curatorFramework.getData().forPath(currentPath)));
            }
        } catch (Exception e) {
            CONSOLE_MONITOR_LOGGER.error("[getAllDcServers]", e);
        }
        return result;
    }

    private final LockInternalsSorter sorter = new LockInternalsSorter() {
        @Override
        public String fixForSorting(String str, String lockName) {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

}
