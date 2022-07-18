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
            logger.info(String.format("**********DataSource %s has been closed,cost:%s ms.**********", name, cost));
            drcTomcatDataSource = null;
            return;
        }

        service.schedule(this, FIXED_DELAY, TimeUnit.MILLISECONDS);
    }

    private boolean closeDataSource(DataSource dataSource) {
        logger.info(String.format("**********Trying to close datasource %s.**********", name));
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

        logger.info(String.format("Error retry times for datasource %s:%s", name, retryTimes));

        int abandonedTimeout = getAbandonedTimeout();
        logger.info(String.format("Abandoned timeout for datasource %s:%s", name, abandonedTimeout));

        int elapsedSeconds = getElapsedSeconds();
        logger.info(String.format("Elapsed seconds for datasource %s:%s", name, elapsedSeconds));

        org.apache.tomcat.jdbc.pool.DataSource ds = drcTomcatDataSource;
        if (retryTimes > MAX_RETRY_TIMES) {
            logger.info(String.format("Force closing datasource %s,retry times:%s,max retry times:%s.", name,
                    retryTimes, MAX_RETRY_TIMES));
            ds.close(true);
            return success;
        } else if (elapsedSeconds >= abandonedTimeout) {
            logger.info(String.format("Force closing datasource %s,elapsed seconds:%s,abandoned timeout:%s.", name,
                    elapsedSeconds, abandonedTimeout));
            ds.close(true);
            return success;
        }

        ConnectionPool pool = ds.getPool();
        if (pool == null)
            return success;

        int idle = pool.getIdle();
        if (idle > 0) {
            pool.purge();
            logger.info(String.format("Idle connections of datasource %s have been closed.", name));
        }

        int active = pool.getActive();
        if (active == 0) {
            ds.close();
            logger.info(
                    String.format("Active connections of datasource %s is zero, datasource has been closed.", name));
        } else if (active > 0) {
            logger.info(String.format("Active connections of datasource %s is %s.", name, active));
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
