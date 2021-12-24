package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Created by mingdongli
 * 2019/11/21 下午10:44.
 */
public abstract class AbstractQueryTask<V> implements Callable<V> {

    protected static final String ALI_RDS = "/*FORCE_MASTER*/";

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
