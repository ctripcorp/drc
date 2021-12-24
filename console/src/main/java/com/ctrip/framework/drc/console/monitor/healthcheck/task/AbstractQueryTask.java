package com.ctrip.framework.drc.console.monitor.healthcheck.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-25
 */
public abstract class AbstractQueryTask<V> implements Callable<V> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DataSourceManager dataSourceManager = DataSourceManager.getInstance();

    protected Endpoint master;

    public AbstractQueryTask(Endpoint master) {
        this.master = master;
    }

    @Override
    public V call() {
        return doQuery();
    }

    abstract protected V doQuery();

}