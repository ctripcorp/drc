package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.QueryLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.ApplyResult;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.parse.DdlParser;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.parse.DdlResult;
import com.ctrip.framework.drc.replicator.impl.monitor.MonitorManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
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
    public static final String RENAME_TABLE = "RENAME TABLE `%s`.`%s` TO `%s`.`%s`";
    private static final String DRC_REPLICATOR_DDL_EXCLUDE_EVENT_TYPE = "DRC.replicator.ddl.exclude";

    private SchemaManager schemaManager;

    private MonitorManager monitorManager;

    private String registryKey;

    private boolean parseDrcDdl = false;

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
            boolean ddlDone = parseQueryEvent(queryLogEvent, value.getGtid());
            if (parseDrcDdl && ddlDone) {
                DDL_LOGGER.info("[DRC DDL] stop parse drc ddl event after encounter mysql native ddl {}", queryLogEvent.getQuery());
                parseDrcDdl = false;
            }
        } else if (drc_schema_snapshot_log_event == logEventType) { // init only first time
            DrcSchemaSnapshotLogEvent snapshotLogEvent = (DrcSchemaSnapshotLogEvent) logEvent;
            parseDrcDdl = schemaManager.shouldRecover(false);
            if (parseDrcDdl) {
                DDL_LOGGER.info("[DRC DDL] need recovery, start parse drc ddl");
                schemaManager.recovery(snapshotLogEvent, false);
            }
            value.mark(OTHER_F);
        } else if (drc_ddl_log_event == logEventType) {
            if (parseDrcDdl) {
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
        List<DdlResult> allResults = DdlParser.parse(queryString, schemaName);
        List<DdlResult> results = this.filterDdlResult(queryString, allResults);
        if (results.isEmpty()) {
            return false;
        }
        queryString = this.updateQueryStringIfNeeded(queryString, charset, results);
        Pair<Boolean, Boolean> pair = this.filterAndProcessExcludedDb(queryString, gtid, results);
        if (Boolean.TRUE.equals(pair.getKey())) {
            return Boolean.TRUE.equals(pair.getValue());
        }

        DdlResult ddlResult = results.get(0);
        QueryType type = ddlResult.getType();

        // rename: target schema name
        // other: schema name
        schemaName = ddlResult.getSchemaName();
        String schemaInBinlog = ddlResult.getOriSchemaName() != null ? ddlResult.getOriSchemaName() : schemaName;
        String tableName = ddlResult.getTableName();
        ApplyResult applyResult = schemaManager.apply(schemaInBinlog, tableName, queryString, type, gtid);
        if (ApplyResult.Status.PARTITION_SKIP == applyResult.getStatus()) {
            DDL_LOGGER.info("[Apply] skip DDL {} for table partition in {}", queryString, getClass().getSimpleName());
            return false;
        }
        if (StringUtils.isBlank(schemaName) || StringUtils.isBlank(tableName)) {
            DDL_LOGGER.info("[Skip] ddl for one of blank {}.{} with query {}", schemaName, tableName, queryString);
            return false;
        }
        queryString = applyResult.getDdl();
        schemaManager.persistDdl(schemaInBinlog, tableName, queryString);
        schemaManager.refresh(getRelatedTables(results));
        DDL_LOGGER.info("[Apply] DDL {} with result {}", queryString, applyResult);

        // persistColumnInfo
        if (mayGhostOps(tableName)) {
            // deal with ghost
            if (type != QueryType.RENAME || results.size() != 2) {
                return false;
            }
            String tableNameOne = ddlResult.getTableName();
            String originTableNameTwo = results.get(1).getOriTableName();
            if (!DDLPredication.mayGhostRename(tableNameOne, originTableNameTwo)) {
                return false;
            }
            schemaName = results.get(1).getSchemaName();
            if (StringUtils.isNotBlank(schemaName)) {
                schemaName = schemaName.toLowerCase();
            }
            tableName = results.get(1).getTableName();
            doPersistColumnInfo(schemaName, tableName, queryString);
            DDL_LOGGER.info("[Rename] detected for {}.{} to go to persist drc table map and ddl event", schemaName, tableName);
            return true;
        } else {
            // fix ddl: use drc1; rename table test1 to drc2.test1;
            String persistSchemaName = schemaName;
            if (type == QueryType.RENAME && results.size() == 1 && StringUtils.isNotBlank(ddlResult.getSchemaName())) {
                persistSchemaName = ddlResult.getSchemaName();
            }
            doPersistColumnInfo(persistSchemaName, tableName, queryString);
            return true;
        }
    }

    /**
     *
     * <table style="border:1px solid">
     * <caption>Parse and apply excluded db ddl</caption>
     *  <thead style="border:1px solid">
     *  <tr>
     *    <th scope="col" > Origin ddl</th>
     *    <th scope="col" > Convert to</th>
     *  </tr>
     *  </thead>
     *  <tbody>
     *  <tr  style="border:1px solid">
     *    <td>rename table drc1.ddl_test_table to configdb.xxx</td>
     *    <td>DROP TABLE IF EXISTS `drc1`.`ddl_test_table`</td>
     *  </tr>
     *  <tr>
     *    <td rowspan="2">rename table drc1.ddl_test_table to configdb.xxx, drc1._mytmp_ddl_test_table to drc1.ddl_test_table</td>
     *    <td>DROP TABLE IF EXISTS `drc1`.`ddl_test_table`</td>
     *  </tr>
     *  <tr>
     *    <td>RENAME TABLE `drc1`.`_mytmp_ddl_test_table` TO `drc1`.`ddl_test_table`</td>
     *  </tr>
     *  </tbody>
     * </table>
     * @return processExcludedDB, persistAnyTableMap
     **/
    private Pair<Boolean, Boolean> filterAndProcessExcludedDb(String queryString, String gtid, List<DdlResult> results) {
        boolean processExcludedDB = false;
        boolean persistAnyTableMap = false;

        List<DdlResult> excludedDbDdls = results.stream()
                .filter(e -> StringUtils.isNotBlank(e.getSchemaName()))
                .filter(e -> EXCLUDED_DB.contains(e.getSchemaName().toLowerCase()))
                .collect(Collectors.toList());
        if (excludedDbDdls.isEmpty()) {
            return Pair.of(processExcludedDB, persistAnyTableMap);
        }

        // 1. need process excluded db ddl
        processExcludedDB = true;
        // 2. rename xxx_db.table to configdb.xxx -> drop table xxx_db.table
        for (DdlResult ddlResult : excludedDbDdls) {
            String originTableName = ddlResult.getOriTableName();
            if (ddlResult.getType() != QueryType.RENAME) {
                DDL_LOGGER.info("[Skip] ddl for exclude database {} with query {}", ddlResult.getSchemaName(), queryString);
                continue;
            }
            String targetSchema = ddlResult.getOriSchemaName() != null ? ddlResult.getOriSchemaName() : ddlResult.getSchemaName();
            if (StringUtils.isBlank(targetSchema) || EXCLUDED_DB.contains(targetSchema.toLowerCase())) {
                continue;
            }
            String dropQuery = String.format(DROP_TABLE, targetSchema, originTableName);
            DDL_LOGGER.info("[Apply] {} for excluded db from ddl {}", dropQuery, queryString);
            schemaManager.apply(targetSchema, originTableName, dropQuery, QueryType.ERASE, gtid);
            schemaManager.persistDdl(targetSchema, originTableName, dropQuery);
            DefaultEventMonitorHolder.getInstance().logEvent(DRC_REPLICATOR_DDL_EXCLUDE_EVENT_TYPE, "drop");
        }
        schemaManager.refresh(getRelatedTables(excludedDbDdls));
        results.removeAll(excludedDbDdls);
        if (results.isEmpty()) {
            return Pair.of(processExcludedDB, persistAnyTableMap);
        }

        // for a single ddl event, only rename has multiple ddl to execute
        if (!results.stream().allMatch(e -> e.getType() == QueryType.RENAME)) {
            DDL_LOGGER.warn("[Apply][Skip][NOT RENAME] for excluded db from ddl {}", queryString);
            DefaultEventMonitorHolder.getInstance().logEvent(DRC_REPLICATOR_DDL_EXCLUDE_EVENT_TYPE, "fail:not rename");
            return Pair.of(processExcludedDB, persistAnyTableMap);
        }
        // 3. apply rename
        for (DdlResult result : results) {
            String targetSchema = result.getSchemaName();
            String targetTableName = result.getTableName();
            String oriTableName = result.getOriTableName();
            String renameQuery = String.format(RENAME_TABLE, result.getOriSchemaName(), oriTableName, targetSchema, targetTableName);
            DDL_LOGGER.info("[Apply] {} for excluded db from ddl {}", renameQuery, queryString);

            schemaManager.apply(targetSchema, targetTableName, renameQuery, QueryType.RENAME, gtid);
            schemaManager.persistDdl(targetSchema, targetTableName, renameQuery);
            schemaManager.refresh(getRelatedTables(Lists.newArrayList(result)));
            doPersistColumnInfo(targetSchema, targetTableName, renameQuery);
            persistAnyTableMap = true;
            DefaultEventMonitorHolder.getInstance().logEvent(DRC_REPLICATOR_DDL_EXCLUDE_EVENT_TYPE, "rename");
        }

        return Pair.of(processExcludedDB, persistAnyTableMap);

    }

    private String updateQueryStringIfNeeded(String queryString, String charset, List<DdlResult> results) {
        if (results.size() == 1) {
            DdlResult ddlResult = results.get(0);
            if (QueryType.CREATE == ddlResult.getType() && ddlResult.getTableCharset() == null && !DEFAULT_CHARACTER_SET_SERVER.equalsIgnoreCase(charset)) {  //not set and serverCollation != DEFAULT_CHARACTER_SET_SERVER
                String previousQueryString = queryString;
                queryString = DdlParser.appendTableCharset(queryString, charset);
                logger.info("[Create] table sql transfer from {} to {}", previousQueryString, queryString);
            }
        }
        return queryString;
    }

    private List<DdlResult> filterDdlResult(String queryString, List<DdlResult> results) {
        return results.stream().filter(ddlResult -> {
            if (!ddlResult.getType().isDmlOrTruncate()) {
                return true;
            }
            // dml or truncate, should skip
            QueryType type = ddlResult.getType();
            if (type == QueryType.TRUNCATE) {
                String tableName = ddlResult.getTableName();
                DDL_LOGGER.info("[Truncate] detected for {}.{}", ddlResult.getSchemaName(), tableName);
                monitorManager.onDdlEvent(ddlResult.getSchemaName(), tableName, queryString, type);
            }
            return false;
        }).collect(Collectors.toList());
    }

    public boolean parseQueryEvent(QueryLogEvent event, String gtid) {
        String queryString = event.getQuery();
        if (StringUtils.startsWithIgnoreCase(queryString, BEGIN) ||
                StringUtils.startsWithIgnoreCase(queryString, COMMIT) ||
                StringUtils.startsWithIgnoreCase(queryString, XA_COMMIT) ||
                StringUtils.startsWithIgnoreCase(queryString, XA_ROLLBACK) ||
                StringUtils.endsWithIgnoreCase(queryString, XA_START) ||
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

    private String getServerCollation(int serverCollation) {
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

    @VisibleForTesting
    protected List<TableId> getRelatedTables(List<DdlResult> results) {
        return results.stream()
                .flatMap(e -> Stream.of(
                        new TableId(e.getSchemaName(), e.getTableName()),
                        new TableId(e.getOriSchemaName() != null ? e.getOriSchemaName() : e.getSchemaName(), e.getOriTableName()))
                )
                .filter(e -> !StringUtils.isEmpty(e.getDbName()) && !StringUtils.isEmpty(e.getTableName()))
                .map(e -> new TableId(e.getDbName().toLowerCase(), e.getTableName().toLowerCase()))
                .filter(e -> !EXCLUDED_DB.contains(e.getDbName()))
                .distinct()
                .collect(Collectors.toList());
    }
}
