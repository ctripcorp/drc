package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.manager.exception.DdlException;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.driver.binlog.manager.TableOperationManager.transformTableComment;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeApplyTask extends AbstractSchemaTask<Boolean> implements NamedCallable<Boolean> {

    public static final String DDL_SQL = "use %s;";

    private String schema;

    private String table;

    private String ddl;

    private String gtid;

    private QueryType queryType;

    private ExecutorService ddlMonitorExecutorService;

    private BaseEndpointEntity baseEndpointEntity;

    public SchemeApplyTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource,
                           String schema, String table, String ddl, String gtid,
                           QueryType queryType, ExecutorService ddlMonitorExecutorService,
                           BaseEndpointEntity baseEndpointEntity) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.schema = schema;
        this.table = table;
        this.ddl = ddl;
        this.gtid = gtid;
        this.queryType = queryType;
        this.ddlMonitorExecutorService = ddlMonitorExecutorService;
        this.baseEndpointEntity = baseEndpointEntity;
    }

    @Override
    public void afterException(Throwable t) {
        super.afterException(t);
        DDL_LOGGER.warn("apply {} failed {}", ddl, t);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.ddl.failed", String.format("DDL:%s\nEXCEPTION:%s", ddl, t.getCause()));
    }

    @Override
    public Boolean call() throws Exception {
        boolean res = true;
        if (!StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "flush")
                && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "grant")
                && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "revoke")
                && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "create user")
                && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "alter user")
                && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "drop user")) {
            Pair<Boolean, GtidSet> contained = shouldExecute();
            if (contained.getKey()) {
                return true;
            }
            if (StringUtils.isNotEmpty(schema)) {
                doExecute(String.format(DDL_SQL, schema));
            }

            Pair<Boolean, String> commentedDdl = transformTableComment(ddl, queryType, contained.getValue().toString());
            DDL_LOGGER.info("[Apply] {} transformed from {}", commentedDdl.getValue(), ddl);
            res = doExecute(commentedDdl.getValue());
            ddlMonitorExecutorService.submit(() -> {
                try {
                    DefaultReporterHolder.getInstance().reportAlterTable(baseEndpointEntity, 1L);
                } catch (Exception e) {
                    DDL_LOGGER.error("[Reporter] error for {}", commentedDdl.getValue(), e);
                }
            });
        }
        return res;
    }

    private Pair<Boolean, GtidSet> shouldExecute() {
        String gtids = new RetryTask<>(new CommentQueryTask(schema, table, inMemoryEndpoint, inMemoryDataSource)).call();
        GtidSet gtidSet = new GtidSet(gtids);
        boolean executed = gtidSet.isContainedWithin(gtid);
        if (!executed) {
            gtidSet = gtidSet.expandTo(gtid);
        } else {
            DDL_LOGGER.info("[Apply] skip ddl {} due to executed gtid {} contained in {}", ddl, gtid, gtids);
        }
        return Pair.from(executed, gtidSet);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private boolean doExecute(String query) throws DdlException {
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                return statement.execute(query);
            }
        } catch (SQLException e) {
            String message = e.getMessage();
            if (StringUtils.isNotBlank(message) && message.contains("Unknown database") && StringUtils.isNotBlank(query) && query.contains("use")) {
                DDL_LOGGER.info("[Ignore] sql '{}' exception", query);
                return true;
            }
            if (StringUtils.isNotBlank(message) && message.contains("No database selected") && StringUtils.isNotBlank(query) && query.contains("/* generated by server */")) {
                DDL_LOGGER.info("[Ignore] sql '{}' exception", query);
                return true;
            }
            DDL_LOGGER.error("Execute sql {} error", query, e);
            throw new DdlException(e);
        }

    }

    @VisibleForTesting
    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    @VisibleForTesting
    public String getDdl() {
        return ddl;
    }
}
