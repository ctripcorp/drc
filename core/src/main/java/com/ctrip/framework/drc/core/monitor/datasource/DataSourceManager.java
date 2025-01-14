package com.ctrip.framework.drc.core.monitor.datasource;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.KeyedEndPoint;
import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.JDBC_URL_FORMAT;

/**
 * Created by mingdongli
 * 2019/11/21 下午3:21.
 */
public class DataSourceManager extends AbstractDataSource {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static final int MAX_ACTIVE = 50;
    public static final int DB_MAX_ACTIVE = 5;
    private Map<Endpoint, Lock> cachedLocks = new ConcurrentHashMap<>();
    private Map<Endpoint, Lock> writeCachedLocks = new ConcurrentHashMap<>();
    private Map<Endpoint, Lock> accValidateCachedLocks = new ConcurrentHashMap<>();

    private static class DataSourceManagerHolder {
        public static final DataSourceManager INSTANCE = new DataSourceManager();
    }

    public static DataSourceManager getInstance() {
        return DataSourceManagerHolder.INSTANCE;
    }

    private DataSourceManager() {
    }

    private Map<Endpoint, DataSource> dataSourceMap = Maps.newConcurrentMap();
    private Map<Endpoint, DataSource> writeDataSourceMap = Maps.newConcurrentMap();
    private Map<Endpoint, DataSource> accValidateDataSourceMap = Maps.newConcurrentMap();

    public DataSource getDataSource(Endpoint endpoint) {
        return this.getDataSource(endpoint, null);
    }

    public DataSource getDataSource(Endpoint endpoint, PoolProperties poolProperties) {
        Lock lock = cachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = dataSourceMap.get(endpoint);
            if (dataSource == null) {
                if (poolProperties == null) {
                    poolProperties = getDefaultPoolProperties(endpoint);
                }
                logger.info("[DataSource] create for {} with connection properties({})", endpoint.getSocketAddress(), poolProperties.getConnectionProperties());
                setCommonProperty(poolProperties);
                configureMonitorProperties(endpoint, poolProperties);
                dataSource = new DrcTomcatDataSource(poolProperties);
                dataSourceMap.put(endpoint, dataSource);
            }
            return dataSource;
        } finally {
            lock.unlock();
        }
    }

    public DataSource getDataSourceForWrite(Endpoint endpoint, PoolProperties poolProperties) {
        Lock lock = writeCachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = writeDataSourceMap.get(endpoint);
            if (dataSource == null) {
                if (poolProperties == null) {
                    poolProperties = getDefaultPoolProperties(endpoint);
                }
                logger.info("[DataSource] create for {} with connection properties({})", endpoint.getSocketAddress(), poolProperties.getConnectionProperties());
                setCommonProperty(poolProperties);
                configureMonitorProperties(endpoint, poolProperties);
                dataSource = new DrcTomcatDataSource(poolProperties);
                writeDataSourceMap.put(endpoint, dataSource);
            }
            return dataSource;
        } finally {
            lock.unlock();
        }
    }

    public static PoolProperties getDefaultPoolProperties(Endpoint endpoint) {
        PoolProperties poolProperties = new PoolProperties();
        String jdbcUrl = String.format(JDBC_URL_FORMAT, endpoint.getHost(), endpoint.getPort());
        poolProperties.setUrl(jdbcUrl);

        poolProperties.setUsername(endpoint.getUser());
        poolProperties.setPassword(endpoint.getPassword());
        poolProperties.setMaxActive(getMaxActive(endpoint));
        poolProperties.setMaxIdle(2);
        poolProperties.setMinIdle(1);
        poolProperties.setInitialSize(1);
        poolProperties.setMaxWait(10000);
        poolProperties.setMaxAge(28000000);
        int socketTimeout = DynamicConfig.getInstance().getDatasourceSocketTimeout();
        String timeout = String.format("connectTimeout=%s;socketTimeout=%s", CONNECTION_TIMEOUT, socketTimeout);
        poolProperties.setConnectionProperties(timeout);

        poolProperties.setValidationInterval(30000);

        return poolProperties;
    }

    protected static int getMaxActive(Endpoint endpoint) {
        if (endpoint instanceof KeyedEndPoint) {
            String registryKey = ((KeyedEndPoint) endpoint).getKey();
            boolean isDbRegistryKey = RegistryKey.getTargetDB(registryKey) != null;
            if (isDbRegistryKey) {
                return DB_MAX_ACTIVE;
            }
        }
        return MAX_ACTIVE;
    }

    public void clearDataSource(Endpoint endpoint) {
        Lock lock = cachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = dataSourceMap.remove(endpoint);
            if (dataSource != null) {
                dataSource.close(true);
                logger.info("[DataSource] close for {}", endpoint.getSocketAddress());
            }
        } finally {
            lock.unlock();
        }
    }

    public void clearDataSourceForWrite(Endpoint endpoint) {
        Lock lock = writeCachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = writeDataSourceMap.remove(endpoint);
            if (dataSource != null) {
                dataSource.close(true);
                logger.info("[DataSource] close for {}", endpoint.getSocketAddress());
            }
        } finally {
            lock.unlock();
        }
    }

    public DataSource getDataSourceForAccountValidate(Endpoint endpoint, PoolProperties poolProperties) {
        Lock lock = accValidateCachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = accValidateDataSourceMap.get(endpoint);
            if (dataSource == null) {
                if (poolProperties == null) {
                    poolProperties = getDefaultPoolProperties(endpoint);
                }
                logger.info(
                        "[DataSource] create for {} with connection properties({})",
                        endpoint.getSocketAddress().toString() + endpoint.getUser()
                        , poolProperties.getConnectionProperties()
                );
                setCommonProperty(poolProperties);
                configAccountValidateProperties(endpoint, poolProperties);
                dataSource = new DrcTomcatDataSource(poolProperties);
                accValidateDataSourceMap.put(endpoint, dataSource);
            }
            return dataSource;
        } finally {
            lock.unlock();
        }
    }

    public void clearDataSourceForAccountValidate(Endpoint endpoint) {
        Lock lock = accValidateCachedLocks.computeIfAbsent(endpoint, key -> new ReentrantLock());
        lock.lock();
        try {
            DataSource dataSource = accValidateDataSourceMap.remove(endpoint);
            if (dataSource != null) {
                dataSource.close(true);
                logger.info("[DataSource] close for {}", endpoint.getSocketAddress());
            }
        } finally {
            lock.unlock();
        }
    }
}
