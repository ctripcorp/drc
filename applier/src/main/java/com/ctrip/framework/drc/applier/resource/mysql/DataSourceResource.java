package com.ctrip.framework.drc.applier.resource.mysql;

import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.resource.thread.Executor;
import com.ctrip.framework.drc.fetcher.system.AbstractResource;
import com.ctrip.framework.drc.fetcher.system.InstanceActivity;
import com.ctrip.framework.drc.fetcher.system.InstanceConfig;
import com.ctrip.framework.drc.fetcher.system.InstanceResource;
import com.ctrip.xpipe.api.monitor.Task;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.monitor.datasource.AbstractDataSource.setCommonProperty;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;

/**
 * @Author Slight
 * May 14, 2020
 */
public class DataSourceResource extends AbstractResource implements DataSource {

    @InstanceActivity
    public MetricsActivity reporter;

    @InstanceConfig(path = "target.URL")
    public String URL;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @InstanceResource
    public Executor executor;

    //100 for apply activity, 1 for merge opposite gtid set
    @InstanceConfig(path = "target.poolSize")
    public int poolSize = 100 + 1;

    public int validationInterval = 30000;

    @InstanceConfig(path = "cluster")
    public String cluster = "unset";

    private PoolProperties properties;

    private javax.sql.DataSource inner;

    private ScheduledExecutorService scheduledExecutorService;

    @Override
    protected void doInitialize() throws Exception {
        properties = new PoolProperties();
        properties.setName(cluster);
        properties.setUrl(URL);
        properties.setUsername(username);
        properties.setPassword(password);
        properties.setDefaultAutoCommit(false);
        String timeout = String.format("connectTimeout=%s;socketTimeout=60000", CONNECTION_TIMEOUT);
        properties.setConnectionProperties(timeout);

        properties.setValidationInterval(validationInterval);
        setCommonProperty(properties);

        properties.setMaxActive(poolSize);
        properties.setMaxIdle(poolSize);
        properties.setInitialSize(30);
        properties.setMinIdle(poolSize);

        inner = new DrcTomcatDataSource(properties);

        logger.info("[INIT DataSource] {}", URL);

        executor.execute(()-> {
            warmUp();
        });

        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(cluster);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(!Thread.currentThread().isInterrupted()) {
                int active = ((DrcTomcatDataSource) inner).getActive();
                if (reporter != null) {
                    reporter.report("jdbc.active", "", active);
                }
            }
        }, 100, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    protected synchronized void doDispose() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
        if (inner != null) {
            ((DrcTomcatDataSource) inner).close(true);
            inner = null;
        }
    }

    public DataSource wrap(javax.sql.DataSource dataSource) {
        this.inner = dataSource;
        return this;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return inner.getConnection();
    }

    private void warmUp() {
        try {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.applier.connection.create", cluster, new Task() {
                @Override
                public void go() throws SQLException {
                    logger.info("[Init] connection for {}:{} begin", cluster, URL);
                    Connection connection = getConnection();
                    if (connection != null) {
                        connection.close();
                    }
                    logger.info("[Init] connection for {}:{} end", cluster, URL);
                }
            });
        } catch (Exception e) {
            logger.error("[Init] connection for {}:{} error", cluster, URL);
        }
    }
}
