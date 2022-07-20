package com.ctrip.framework.drc.applier.resource.mysql;

import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.utils.OsUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/7/18
 */
public class DataSourceTerminator {

    private static final int INIT_DELAY = 0;

    private static volatile DataSourceTerminator terminator = null;

    private ScheduledExecutorService service = ThreadUtils.newFixedThreadScheduledPool(OsUtils.getCpuCount(), "DataSourceTerminator");

    public synchronized static DataSourceTerminator getInstance() {
        if (terminator == null) {
            terminator = new DataSourceTerminator();
        }

        return terminator;
    }

    public void close(DrcTomcatDataSource dataSource) {
        DataSourceTerminateTask task = new DataSourceTerminateTask(dataSource);;
        task.setScheduledExecutorService(service);
        service.schedule(task, INIT_DELAY, TimeUnit.MILLISECONDS);
    }
}
