package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeApplyTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeClearTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeCloneTask;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by mingdongli
 * 2019/9/29 下午8:27.
 */
public abstract class AbstractSchemaManager extends AbstractLifecycle implements SchemaManager {

    protected static final Logger DDL_LOGGER = LoggerFactory.getLogger("com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter");

    protected Map<TableId, TableInfo> tableInfoMap = Maps.newConcurrentMap();

    protected int port;

    protected Endpoint endpoint;

    protected String clusterName;

    protected BaseEndpointEntity baseEndpointEntity;

    protected ExecutorService ddlMonitorExecutorService = ThreadUtils.newSingleThreadExecutor("MySQLSchemaManager-DDL");

    protected DataSource inMemoryDataSource;

    protected Endpoint inMemoryEndpoint;

    @Override
    public boolean recovery(DrcSchemaSnapshotLogEvent snapshotLogEvent) {
        Map<String, Map<String, String>> ddlSchemas = snapshotLogEvent.getDdls();
        boolean res = doClone(ddlSchemas);
        DDL_LOGGER.info("[Recovery] DrcSchemaSnapshotLogEvent from binlog finished with result {}", res);
        return res;
    }

    protected boolean doClone(Map<String, Map<String, String>> ddlSchemas) {
        new RetryTask<>(new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource)).call();
        Boolean res = new RetryTask<>(new SchemeCloneTask(ddlSchemas, inMemoryEndpoint, inMemoryDataSource)).call();
        return res == null ? false : res.booleanValue();
    }

    @Override
    public boolean apply(String schema, String ddl) {
        tableInfoMap.clear();
        synchronized (this) {
            Boolean res = new RetryTask<>(new SchemeApplyTask(inMemoryEndpoint, inMemoryDataSource, schema, ddl, ddlMonitorExecutorService, baseEndpointEntity)).call();
            return res == null ? false : res.booleanValue();
        }
    }
}
