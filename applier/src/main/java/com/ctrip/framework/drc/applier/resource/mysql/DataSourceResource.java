package com.ctrip.framework.drc.applier.resource.mysql;

import com.ctrip.framework.drc.applier.activity.monitor.MetricsActivity;
import com.ctrip.framework.drc.core.driver.pool.DrcDataSourceValidator;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.core.monitor.datasource.AbstractDataSource.setCommonProperty;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.HEARTBEAT_LOGGER;

/**
 * @Author Slight
 * May 14, 2020
 */
public class DataSourceResource extends AbstractResource implements DataSource {

    @InstanceActivity
    public MetricsActivity reporter;

    @InstanceConfig(path = "target.ip")
    public String ip = "";

    @InstanceConfig(path = "target.URL")
    public String URL;

    @InstanceConfig(path = "target.username")
    public String username;

    @InstanceConfig(path = "target.password")
    public String password;

    @InstanceResource
    public Executor executor;

    @InstanceConfig(path = "target.poolSize")
    public int poolSize = 100;

    public int validationInterval = 30000;

    @InstanceConfig(path = "registryKey")
    public String registryKey = "unset";

    private PoolProperties properties;

    private javax.sql.DataSource inner;

    private ScheduledExecutorService scheduledExecutorService;

    @Override
    protected void doInitialize() throws Exception {
        properties = new PoolProperties();
        properties.setName(registryKey);
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
        properties.setValidator(new DrcDataSourceValidator(properties));

        inner = new DrcTomcatDataSource(properties);

        logger.info("[INIT DataSource] {}", URL);

        executor.execute(()-> {
            warmUp();
        });

        scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(registryKey);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(!Thread.currentThread().isInterrupted()) {
                int active = ((DrcTomcatDataSource) inner).getActive();
                if (reporter != null) {
                    reporter.report("jdbc.active", "", active);
                }
            }
        }, 100, 200, TimeUnit.MILLISECONDS);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                HEARTBEAT_LOGGER.info("{} - HOST - {}", registryKey, InetAddress.getByName(ip));
            } catch (UnknownHostException e) {
                HEARTBEAT_LOGGER.error("{} - HOST ERROR - {}", registryKey, ip);
            }
        }, 60, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doDispose() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
            scheduledExecutorService = null;
        }
        if (inner != null) {
            DataSourceTerminator.getInstance().close((DrcTomcatDataSource) inner);
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
            DefaultTransactionMonitorHolder.getInstance().logTransaction("DRC.applier.connection.create", registryKey, new Task() {
                @Override
                public void go() throws SQLException {
                    logger.info("[Init] connection for {}:{} begin", registryKey, URL);
                    Connection connection = getConnection();
                    if (connection != null) {
                        connection.close();
                    }
                    logger.info("[Init] connection for {}:{} end", registryKey, URL);
                }
            });
        } catch (Exception e) {
            logger.error("[Init] connection for {}:{} error", registryKey, URL);
        }
    }
}
