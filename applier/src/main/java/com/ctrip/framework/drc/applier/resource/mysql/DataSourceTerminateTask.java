package com.ctrip.framework.drc.applier.resource.mysql;

import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.PoolConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/7/18
 */
public class DataSourceTerminateTask implements Runnable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int MAX_RETRY_TIMES = 3;
    private static final int FIXED_DELAY = 5 * 1000; // milliseconds
    private ScheduledExecutorService service = null;

    private DrcTomcatDataSource drcTomcatDataSource;
    private String name;
    private PoolConfiguration poolProperties;
    private Date enqueueTime;
    private int retryTimes;

    public DataSourceTerminateTask(DrcTomcatDataSource drcTomcatDataSource) {
        this.drcTomcatDataSource = drcTomcatDataSource;
        this.name = drcTomcatDataSource.getName();
        this.poolProperties = drcTomcatDataSource.getPoolProperties();
        this.enqueueTime = new Date();
        this.retryTimes = 0;
    }


    public void setScheduledExecutorService(ScheduledExecutorService service) {
        this.service = service;
    }

    @Override
    public void run() {
        boolean success = closeDataSource(drcTomcatDataSource);
        if (success) {
            long cost = getElapsedMilliSeconds();
            logger.info("[DataSource Terminate] {} has been closed,cost:{} ms.", name, cost);
            drcTomcatDataSource = null;
            return;
        }

        service.schedule(this, FIXED_DELAY, TimeUnit.MILLISECONDS);
    }

    private boolean closeDataSource(DataSource dataSource) {
        logger.info("[DataSource Terminate] Trying to close datasource {}.", name);
        boolean success = true;

        try {
            // Tomcat DataSource
            if (dataSource instanceof org.apache.tomcat.jdbc.pool.DataSource) {
                success = closeTomcatDataSource();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            retryTimes++;
            success = false;
        }

        return success;
    }

    private boolean closeTomcatDataSource() {
        boolean success = true;

        logger.info("[DataSource Terminate] Error retry times for datasource {}:{}", name, retryTimes);

        int abandonedTimeout = getAbandonedTimeout();
        logger.info("[DataSource Terminate] Abandoned timeout for datasource {}:{}", name, abandonedTimeout);

        int elapsedSeconds = getElapsedSeconds();
        logger.info("[DataSource Terminate] Elapsed seconds for datasource {}:{}", name, elapsedSeconds);

        org.apache.tomcat.jdbc.pool.DataSource ds = drcTomcatDataSource;
        if (retryTimes > MAX_RETRY_TIMES) {
            logger.info("[DataSource Terminate] Force closing datasource {},retry times:{},max retry times:{}.", name, retryTimes, MAX_RETRY_TIMES);
            ds.close(true);
            return success;
        } else if (elapsedSeconds >= abandonedTimeout) {
            logger.info("[DataSource Terminate] Force closing datasource {},elapsed seconds:{},abandoned timeout:{}.", name, elapsedSeconds, abandonedTimeout);
            ds.close(true);
            return success;
        }

        ConnectionPool pool = ds.getPool();
        if (pool == null)
            return success;

        int idle = pool.getIdle();
        if (idle > 0) {
            pool.purge();
            logger.info("[DataSource Terminate] Idle connections of datasource {} have been closed.", name);
        }

        int active = pool.getActive();
        if (active == 0) {
            ds.close();
            logger.info("[DataSource Terminate] Active connections of datasource {} is zero, datasource has been closed.", name);
        } else if (active > 0) {
            logger.info("[DataSource Terminate] Active connections of datasource {} is {}.", name, active);
            success = false;
        }

        return success;
    }

    private int getAbandonedTimeout() {
        return poolProperties.getRemoveAbandonedTimeout();
    }

    private long getElapsedMilliSeconds() {
        return new Date().getTime() - enqueueTime.getTime();
    }

    private int getElapsedSeconds() {
        long elapsed = getElapsedMilliSeconds();
        return (int) (elapsed / 1000);
    }
}
