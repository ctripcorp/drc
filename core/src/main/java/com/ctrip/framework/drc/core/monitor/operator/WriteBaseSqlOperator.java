package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dengquanliang
 * 2023/12/28 11:36
 */
public abstract class WriteBaseSqlOperator  extends AbstractLifecycle implements SqlOperator {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DataSource writeDataSource;

    protected PoolProperties poolProperties;

    protected Endpoint endpoint;

    protected DataSourceManager dataSourceManager = DataSourceManager.getInstance();

    public WriteBaseSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        this.poolProperties = poolProperties;
        this.endpoint = endpoint;
    }

    @Override
    protected void doInitialize() throws Exception {
        writeDataSource = dataSourceManager.getDataSourceForWrite(endpoint, poolProperties);
    }

    @Override
    protected void doStop() throws Exception {
        dataSourceManager.clearDataSourceForWrite(endpoint);
    }

    @Override
    public PoolProperties getPoolProperties() {
        return poolProperties;
    }
}
