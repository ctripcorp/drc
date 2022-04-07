package com.ctrip.framework.drc.core.monitor.datasource;

import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.JDBC_URL_FORMAT;

/**
 * Created by mingdongli
 * 2019/11/21 下午3:21.
 */
public class DataSourceManager extends AbstractDataSource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static class DataSourceManagerHolder {
        public static final DataSourceManager INSTANCE = new DataSourceManager();
    }

    public static DataSourceManager getInstance() {
        return DataSourceManagerHolder.INSTANCE;
    }

    private DataSourceManager() {
    }

    private Map<Endpoint, DataSource> dataSourceMap = Maps.newConcurrentMap();

    public synchronized DataSource getDataSource(Endpoint endpoint) {
        return this.getDataSource(endpoint, null);
    }

    public synchronized DataSource getDataSource(Endpoint endpoint, PoolProperties poolProperties) {

        DataSource dataSource = dataSourceMap.get(endpoint);
        if (dataSource == null) {
            logger.info("[DataSource] create for {}", endpoint.getSocketAddress());
            if (poolProperties == null) {
                poolProperties = getDefaultPoolProperties(endpoint);
            }
            setCommonProperty(poolProperties);
            configureMonitorProperties(endpoint, poolProperties);
            dataSource = new DrcTomcatDataSource(poolProperties);
            dataSourceMap.put(endpoint, dataSource);

        }

        return dataSource;
    }

    public static PoolProperties getDefaultPoolProperties(Endpoint endpoint) {
        PoolProperties poolProperties = new PoolProperties();
        String jdbcUrl = String.format(JDBC_URL_FORMAT, endpoint.getHost(), endpoint.getPort());
        poolProperties.setUrl(jdbcUrl);

        poolProperties.setUsername(endpoint.getUser());
        poolProperties.setPassword(endpoint.getPassword());
        poolProperties.setMaxActive(40);
        poolProperties.setMaxIdle(2);
        poolProperties.setMinIdle(1);
        poolProperties.setInitialSize(1);
        poolProperties.setMaxWait(10000);
        poolProperties.setMaxAge(28000000);
        String timeout = String.format("connectTimeout=%s;socketTimeout=10000", CONNECTION_TIMEOUT);
        poolProperties.setConnectionProperties(timeout);

        poolProperties.setValidationInterval(30000);

        return poolProperties;
    }

    public synchronized void clearDataSource(Endpoint endpoint) {
        DataSource dataSource = dataSourceMap.remove(endpoint);
        if (dataSource != null) {
            dataSource.close(true);
            logger.info("[DataSource] close for {}", endpoint.getSocketAddress());
        }
    }
}
