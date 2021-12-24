package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.monitor.cases.function.DatachangeLastTimeMonitorCase;
import com.ctrip.framework.drc.console.monitor.cases.manager.MySQLMonitorCaseManager;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.monitor.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-03
 */
@Order(2)
@Component
@DependsOn("dbClusterSourceProvider")
public class DefaultMonitorModule extends AbstractMonitor{

    private ScheduledExecutorService mysqlMonitorScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("mysql-scheduledExecutorService");

    private static final int DEFAULT_MONITOR_INITIAL_DELAY = 0;

    private static final int MYSQL_MONITOR_DELAY = 1;

    private MySQLMonitorCaseManager mySQLMonitorCaseManager = new MySQLMonitorCaseManager();

    @Autowired
    private DatachangeLastTimeMonitorCase datachangeLastTimeMonitorCase;

    @PostConstruct
    @Override
    public void start() {
        addPairCase();

        mysqlMonitorScheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                DefaultTransactionMonitorHolder.getInstance().logTransaction("MySQLMonitor", "datachange_lasttime", new Task() {
                    @Override
                    public void go() {
                        mySQLMonitorCaseManager.doMonitor();
                    }
                });
            } catch (Throwable t) {
                logger.error("MySQLMonitor error, ", t);
            }
        }, DEFAULT_MONITOR_INITIAL_DELAY, MYSQL_MONITOR_DELAY, TimeUnit.SECONDS);

    }

    private void addPairCase() {
        mySQLMonitorCaseManager.addCase(datachangeLastTimeMonitorCase);
    }

    @PreDestroy
    @Override
    public void destroy() {
        super.destroy();
        mysqlMonitorScheduledExecutorService.shutdownNow();
    }
}
