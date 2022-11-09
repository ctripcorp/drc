package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.*;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.utils.ThreadUtils.getThreadName;

/**
 * Created by mingdongli
 * 2019/9/29 下午8:27.
 */
public abstract class AbstractSchemaManager extends AbstractLifecycle implements SchemaManager {

    protected static final Logger DDL_LOGGER = LoggerFactory.getLogger("com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter");

    protected Map<TableId, TableInfo> tableInfoMap = Maps.newConcurrentMap();

    public static final int PORT_STEP = 10000;

    protected int port;

    protected Endpoint endpoint;

    protected String registryKey;

    protected BaseEndpointEntity baseEndpointEntity;

    protected ExecutorService ddlMonitorExecutorService;

    protected DataSource inMemoryDataSource;

    protected Endpoint inMemoryEndpoint;

    public AbstractSchemaManager(Endpoint endpoint, int port, String registryKey) {
        this.port = port + PORT_STEP;
        logger.info("[Schema] port is {}", port);
        this.endpoint = endpoint;
        this.registryKey = registryKey;
        ddlMonitorExecutorService = ThreadUtils.newSingleThreadExecutor(getThreadName(getClass().getSimpleName(), registryKey));
    }

    @Override
    public boolean recovery(DrcSchemaSnapshotLogEvent snapshotLogEvent) {
        Map<String, Map<String, String>> future= snapshotLogEvent.getDdls();
        if (isSame(future)) {
            return true;
        }
        boolean res = doClone(future);
        DDL_LOGGER.info("[Recovery] DrcSchemaSnapshotLogEvent from binlog finished with result {} for {}", res, registryKey);
        return res;
    }

    protected boolean doClone(Map<String, Map<String, String>> ddlSchemas) {
        new RetryTask<>(new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource)).call();
        Boolean res = new RetryTask<>(new SchemeCloneTask(ddlSchemas, inMemoryEndpoint, inMemoryDataSource, registryKey)).call();
        return res == null ? false : res.booleanValue();
    }

    /**
     * key : dbName
     * value: List<create table>
     * @return
     */
    protected Map<String, Map<String, String>> doSnapshot(Endpoint endpoint) {
        DataSource dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        Map<String, Map<String, String>> snapshot = new RetryTask<>(new SchemaSnapshotTask(endpoint, dataSource)).call();
        if (snapshot == null) {
            snapshot = Maps.newHashMap();
        }
        return snapshot;
    }

    @Override
    public ApplyResult apply(String schema, String ddl, QueryType queryType) {
        if (QueryType.ALTER == queryType) {
            if (TablePartitionManager.transformAlterPartition(ddl)) {
                return ApplyResult.PARTITION_SKIP;
            }
        } else if (QueryType.CREATE == queryType) {
            Pair<Boolean, String> transformRes = TablePartitionManager.transformCreatePartition(ddl);
            if (transformRes.getKey()) {
                ddl = transformRes.getValue();
            }
        }
        tableInfoMap.clear();
        synchronized (this) {
            Boolean res = new RetryTask<>(new SchemeApplyTask(inMemoryEndpoint, inMemoryDataSource, schema, ddl, ddlMonitorExecutorService, baseEndpointEntity)).call();
            return res == null ? ApplyResult.FAIL : ApplyResult.SUCCESS;
        }
    }

    private boolean isSame(Map<String, Map<String, String>> future) {
        Map<String, Map<String, String>> current = doSnapshot(inMemoryEndpoint);
        boolean same = current.equals(future);
        if (same) {
            DefaultEventMonitorHolder.getInstance().logEvent("Drc.replicator.schema.recovery.bypass", registryKey);
            DDL_LOGGER.info("[Recovery] DrcSchemaSnapshotLogEvent from binlog skip due to no change for {}, current : {}", registryKey, current);
        } else {
            DDL_LOGGER.info("[Recovery] DrcSchemaSnapshotLogEvent from binlog for {}, current : {}, future : {}", registryKey, current, future);
        }
        return same;
    }
}
