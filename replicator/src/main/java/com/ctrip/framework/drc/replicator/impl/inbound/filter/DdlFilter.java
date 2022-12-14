package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.QueryLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.ApplyResult;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.parse.DdlParser;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.parse.DdlResult;
import com.ctrip.framework.drc.replicator.impl.monitor.MonitorManager;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_ddl_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_schema_snapshot_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.query_log_event;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.EXCLUDED_DB;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;
import static com.ctrip.framework.drc.replicator.impl.inbound.filter.TransactionFlags.OTHER_F;
import static com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication.mayGhostOps;

/**
 * @Author limingdong
 * @create 2020/2/24
 */
public class DdlFilter extends AbstractLogEventFilter<InboundLogEventContext> {

    public static final String XA_START = "XA START";

    public static final String XA_END = "XA END";

    public static final String XA_COMMIT = "XA COMMIT";

    public static final String XA_ROLLBACK = "XA ROLLBACK";

    public static final String BEGIN = "BEGIN";

    public static final String COMMIT = "COMMIT";

    public static final String DEFAULT_CHARACTER_SET_SERVER = "utf8mb4";

    public static final String DROP_TABLE = "DROP TABLE IF EXISTS `%s`.`%s`";

    private SchemaManager schemaManager;

    private MonitorManager monitorManager;

    private String registryKey;

    public DdlFilter(SchemaManager schemaManager, MonitorManager monitorManager, String registryKey) {
        this.schemaManager = schemaManager;
        this.monitorManager = monitorManager;
        this.registryKey = registryKey;
    }

    @Override
    public boolean doFilter(InboundLogEventContext value) {
        LogEvent logEvent = value.getLogEvent();
        final LogEventType logEventType = logEvent.getLogEventType();
        if (query_log_event == logEventType) {
            QueryLogEvent queryLogEvent = (QueryLogEvent) logEvent;
            parseQueryEvent(queryLogEvent, value.getGtid());
        } else if (drc_schema_snapshot_log_event == logEventType) { // init only first time
            DrcSchemaSnapshotLogEvent snapshotLogEvent = (DrcSchemaSnapshotLogEvent) logEvent;
            schemaManager.recovery(snapshotLogEvent);
            value.mark(OTHER_F);
        } else if (drc_ddl_log_event == logEventType) {
            if (!DynamicConfig.getInstance().getIndependentEmbeddedMySQLSwitch(registryKey)) {
                DrcDdlLogEvent ddlLogEvent = (DrcDdlLogEvent) logEvent;
                doParseQueryEvent(ddlLogEvent.getDdl(), ddlLogEvent.getSchema(), DEFAULT_CHARACTER_SET_SERVER, value.getGtid());
                DDL_LOGGER.info("[Handle] drc_ddl_log_event of sql {} for {}", ddlLogEvent.getDdl(), registryKey);
            } else {
                DDL_LOGGER.info("[Skip] drc_ddl_log_event for {}", registryKey);
            }
        }

        return doNext(value, value.isInExcludeGroup());

    }

    private boolean doParseQueryEvent(String queryString, String schemaName, String charset, String gtid) {
        List<DdlResult> results = DdlParser.parse(queryString, schemaName);
        if (results.isEmpty()) {
            return false;
        }

        DdlResult ddlResult = results.get(0);
        QueryType type = ddlResult.getType();
        schemaName = ddlResult.getSchemaName();
        String schemaInBinlog = ddlResult.getOriSchemaName() != null ? ddlResult.getOriSchemaName() : schemaName;

        String tableCharset = ddlResult.getTableCharset();
        if (QueryType.CREATE == type && tableCharset == null && !DEFAULT_CHARACTER_SET_SERVER.equalsIgnoreCase(charset)) {  //not set and serverCollation != DEFAULT_CHARACTER_SET_SERVER
            String previousQueryString = queryString;
            queryString = DdlParser.appendTableCharset(queryString, charset);
            logger.info("[Create] table sql transfer from {} to {}", previousQueryString, queryString);
        }

        boolean isDml = (type == QueryType.INSERT || type == QueryType.UPDATE || type == QueryType.DELETE || type == QueryType.TRUNCATE);

        if (!isDml) {
            if (StringUtils.isNotBlank(schemaName) && EXCLUDED_DB.contains(schemaName.toLowerCase())) {
                DDL_LOGGER.info("[Skip] ddl for exclude database {} with query {}", schemaName, queryString);
                String originTableName = ddlResult.getOriTableName();
                if (type == QueryType.RENAME && StringUtils.isNotBlank(schemaInBinlog)
                        && !EXCLUDED_DB.contains(schemaInBinlog.toLowerCase())
                        && mayGhostOps(originTableName)) {
                    String dropQuery = String.format(DROP_TABLE, schemaInBinlog, originTableName);
                    DDL_LOGGER.info("[Apply] {} for excluded db from ddl {}", dropQuery, queryString);
                    schemaManager.apply(schemaInBinlog, originTableName, dropQuery, QueryType.ERASE, gtid);
                }
                return false;
            }
            String tableName = ddlResult.getTableName();
            ApplyResult applyResult = schemaManager.apply(schemaInBinlog, tableName, queryString, type, gtid);
            if (ApplyResult.Status.PARTITION_SKIP == applyResult.getStatus()) {
                DDL_LOGGER.info("[Apply] skip DDL {} for table partition in {}", queryString, getClass().getSimpleName());
                return false;
            }
            queryString = applyResult.getDdl();
            schemaManager.persistDdl(schemaInBinlog, tableName, queryString);
            DDL_LOGGER.info("[Apply] DDL {} with result {}", queryString, applyResult);

            if (StringUtils.isBlank(schemaName) || StringUtils.isBlank(tableName)) {
                DDL_LOGGER.info("[Skip] ddl for one of blank {}.{} with query {}", schemaName, tableName, queryString);
                return false;
            }

            //deal with ghost
            if (mayGhostOps(tableName)) {
                if (type == QueryType.RENAME && results.size() == 2) {
                    String tableNameOne = ddlResult.getTableName();
                    String originTableNameTwo = results.get(1).getOriTableName();
                    if (DDLPredication.mayGhostRename(tableNameOne, originTableNameTwo)) {
                        schemaName = results.get(1).getSchemaName();
                        if (StringUtils.isNotBlank(schemaName)) {
                            schemaName = schemaName.toLowerCase();
                        }
                        tableName = results.get(1).getTableName();
                        doPersistColumnInfo(schemaName, tableName, queryString);
                        DDL_LOGGER.info("[Rename] detected for {}.{} to go to persist drc table map and ddl event", schemaName, tableName);
                        return true;
                    }

                }
                return false;
            } else {
                // fix ddl: use drc1; rename table test1 to drc2.test1;
                String persistSchemaName = schemaName;
                String toSchemaName = ddlResult.getSchemaName();
                if (type == QueryType.RENAME && results.size() == 1 && StringUtils.isNotBlank(toSchemaName)) {
                    persistSchemaName = toSchemaName;
                }
                doPersistColumnInfo(persistSchemaName, tableName, queryString);
            }

            return true;
        } else {
            if(type == QueryType.TRUNCATE) {
                String tableName = ddlResult.getTableName();
                DDL_LOGGER.info("[Truncate] detected for {}.{}", schemaName, tableName);
                monitorManager.onDdlEvent(schemaName, tableName, queryString, type);
            }
        }

        return false;
    }

    public boolean parseQueryEvent(QueryLogEvent event, String gtid) {
        String queryString = event.getQuery();
        if (StringUtils.startsWithIgnoreCase(queryString, BEGIN)      ||
                StringUtils.startsWithIgnoreCase(queryString, COMMIT)     ||
                StringUtils.startsWithIgnoreCase(queryString, XA_COMMIT)  ||
                StringUtils.startsWithIgnoreCase(queryString, XA_ROLLBACK)||
                StringUtils.endsWithIgnoreCase(queryString, XA_START)     ||
                StringUtils.endsWithIgnoreCase(queryString, XA_END)) {
            return false;
        } else {
            QueryLogEvent.QueryStatus queryStatus = event.getQueryStatus();
            String charset = DEFAULT_CHARACTER_SET_SERVER;
            if (queryStatus != null) {
                int serverCollation = queryStatus.getServerCollation();
                charset = getServerCollation(serverCollation);
            }
            return doParseQueryEvent(queryString, event.getSchemaName(), charset, gtid);
        }
    }

    private String getServerCollation(int serverCollation){
        String charset = CharsetConversion.getCharset(serverCollation);
        if (charset == null) {
            return DEFAULT_CHARACTER_SET_SERVER;
        }
        return charset;
    }

    private boolean doPersistColumnInfo(String schemaName, String tableName, String queryString) {
        TableInfo tableInfo = schemaManager.find(schemaName, tableName);
        if (tableInfo != null) {
            schemaManager.persistColumnInfo(tableInfo, false);
            return true;
        } else {
            DDL_LOGGER.info("[Find] column info {}.{} with query {} return NULL", schemaName, tableName, queryString);
            return false;
        }
    }

}
