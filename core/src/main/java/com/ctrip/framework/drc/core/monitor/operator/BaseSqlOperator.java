package com.ctrip.framework.drc.core.monitor.operator;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/11/12 下午5:58.
 */
public abstract class BaseSqlOperator extends AbstractLifecycle implements ReadSqlOperator<ReadResource> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DataSource dataSource;
    protected PoolProperties poolProperties;

    protected Endpoint endpoint;

    protected DataSourceManager dataSourceManager = DataSourceManager.getInstance();

    public BaseSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
        this.poolProperties = poolProperties;
        this.endpoint = endpoint;
    }

    @Override
    protected void doInitialize() throws Exception {
        dataSource = dataSourceManager.getDataSource(endpoint, poolProperties);
    }

    @Override
    protected void doStop() throws Exception {
        dataSourceManager.clearDataSource(endpoint);
    }

    @Override
    public PoolProperties getPoolProperties() {
        return poolProperties;
    }

}
