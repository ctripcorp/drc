package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.*;
import static com.ctrip.framework.drc.core.service.utils.Constants.DRC_MONITOR_SCHEMA_TABLE;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-25
 */
public class MySqlUtils {

    protected static Logger logger = LoggerFactory.getLogger("tableConsistencyMonitorLogger");

    private static Map<Endpoint, WriteSqlOperatorWrapper> sqlOperatorMapper = new HashMap<>();

    public static final String GET_DEFAULT_TABLES = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'mysql', 'sys', 'performance_schema', 'configdb')  AND table_type not in ('view') AND table_schema NOT LIKE '\\_%' AND table_name NOT LIKE '\\_%';";

    public static final String GET_APPROVED_TRUNCATE_TABLES = "select db_name, table_name from configdb.approved_truncatelist;";
    
    public static final String DRC_MONITOR_DB = "drcmonitordb";

    private static final String GET_CREATE_TABLE_STMT = "SHOW CREATE TABLE %s";

    private static final int CREATE_TABLE_INDEX = 2;

    private static final String GET_DB_TABLES_PREFIX = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables WHERE table_schema IN (%s)";

    private static final String GET_DB_TABLES_SUFFIX = " AND table_name NOT LIKE '\\_%' GROUP BY table_schema, table_name;";

    private static final String MATCH_ALL_FILTER = ".*";


    /**
     * CHECK MySql Config 
     * log_bin = ON/1
     * binlog_format = ROW
     * BINLOG_TRANSACTION_DEPENDENCY_TRACKING = WRITESET
     * log_bin_use_v1_row_events=OFF
     * GTID_MODE=ON
     * auto_increment_increment depend on DRC deploy
     * auto_increment_offset depend on DRC deploy
     * drcmonitordb should have 2 tables : [delaymonitor,gtid_executed]
     * table via drc sync should have [pk/uk, column on_update(eg:datachange_lasttime), index in on_update column]
     * table via drc sync forbid truncate
     */
    private static final String CHECK_BINLOG = "show global variables like \"log_bin\";";
    private static final String CHECK_BINLOG_FORMAT = "show global variables like \"binlog_format\";";
    private static final String CHECK_BINLOG_TRANSACTION_DEPENDENCY_TRACKING = "SELECT @@BINLOG_TRANSACTION_DEPENDENCY_TRACKING;";
    private static final String CHECK_BINLOG_VERSION1 = "show global variables like \"log_bin_use_v1_row_events\";";
    private static final String CHECK_GTID_MODE = "SELECT @@GTID_MODE;";
    private static final String CHECK_INCREMENT_STEP = "show global variables like \"auto_increment_increment\";";
    private static final String CHECK_INCREMENT_OFFSET = "show global variables like \"auto_increment_offset\";";
    private static final String CHECK_DRC_TABLES = "select count(*) from information_schema.tables where TABLE_SCHEMA = \"drcmonitordb\";";
    private static final int SHOW_CERTAIN_VARIABLES_INDEX = 2;
    private static final String CHECK_ACCOUNT_AVAILABLE = "SELECT @@GTID_MODE;";
    private static final String BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE = "show global variables like \"binlog_transaction_dependency_history_size\";";
    private static final int BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX = 2;

    private static final String ON_UPDATE = "on update";
    private static final String PRIMARY_KEY = "primary key";
    private static final String UNIQUE_KEY = "unique key";
    private static final String DEFAULT_ZERO_TIME = "0000-00-00 00:00:00";

    private static final String GET_COLUMN_PREFIX = "select column_name from information_schema.columns where table_schema='%s' and table_name='%s'";
    private static final String GET_ALL_COLUMN_PREFIX = "select group_concat(column_name) from information_schema.columns where table_schema='%s' and table_name='%s'";
    private static final String GET_PRIMARY_KEY_COLUMN = " and column_key='PRI';";
    private static final String GET_ON_UPDATE_COLUMN = " and COLUMN_TYPE in ('timestamp(3)','datetime(3)') and EXTRA like 'on update%';";
    private static final int COLUMN_INDEX = 1;
    
    private static final String GTID_EXECUTED_COMMAND_V2 = "show global variables like \"gtid_executed\";";
    private static final int GTID_EXECUTED_INDEX_V2 = 2;
    
    public static List<TableSchemaName> getDefaultTables(Endpoint endpoint) {
        return getTables(endpoint, GET_DEFAULT_TABLES, false);
    }

    public static List<TableSchemaName> getTables(Endpoint endpoint, String sql, Boolean removeSqlOperator) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        List<TableSchemaName> tables = Lists.newArrayList();
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            while(rs.next()) {
                tables.add(new TableSchemaName(rs.getString(1), rs.getString(2)));
            }
        } catch(Throwable t) {
            logger.error("[[monitor=table,endpoint={}:{}]] getTables error: ", endpoint.getHost(), endpoint.getPort(), t);
            removeSqlOperator(endpoint);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
            if(removeSqlOperator) {
                removeSqlOperator(endpoint);
            }
        }
        return tables;
    }

    /**
     * @param endpoint
     * @return key: database.table, value: createTblStmts
     */
    public static Map<String, String> getDefaultCreateTblStmts(Endpoint endpoint, AviatorRegexFilter aviatorRegexFilter) {
        List<TableSchemaName> tables = getDefaultTables(endpoint);
        return getCreateTblStmts(endpoint,
                tables.stream().
                        filter(tableSchemaName-> aviatorRegexFilter.filter(tableSchemaName.getDirectSchemaTableName())).
                        map(TableSchemaName::getDirectSchemaTableName).collect(Collectors.toList()), false);
    }
    public static List<TableSchemaName> getTablesAfterRegexFilter(Endpoint endpoint, AviatorRegexFilter aviatorRegexFilter) {
        List<TableSchemaName> tables = getDefaultTables(endpoint);
        return tables.stream().
                filter(tableSchemaName -> aviatorRegexFilter.filter(tableSchemaName.getDirectSchemaTableName()) 
                        && !tableSchemaName.getSchema().equals(DRC_MONITOR_DB)).
                collect(Collectors.toList());
    }

    /**
     * @param endpoint
     * @return key: database.table, value: DelayMonitorConfig objects
     */
    public static Map<String, DelayMonitorConfig> getDefaultDelayMonitorConfigs(Endpoint endpoint) {
        List<TableSchemaName> tableSchemaNames = getDefaultTables(endpoint);
        return getDelayMonitorConfigs(endpoint, tableSchemaNames, false);
    }

    public static Map<String, DelayMonitorConfig> getDelayMonitorConfigs(Endpoint endpoint, List<TableSchemaName> tableSchemaNames, Boolean removeSqlOperator) {
        Map<String, DelayMonitorConfig> delayMonitorConfigs = Maps.newHashMap();
        for(TableSchemaName tableSchemaName : tableSchemaNames) {
            String tableSchema = tableSchemaName.toString();
            if (DRC_MONITOR_SCHEMA_TABLE.equalsIgnoreCase(tableSchema)) {
                continue;
            }
            DelayMonitorConfig delayMonitorConfig = new DelayMonitorConfig();
            delayMonitorConfig.setSchema(tableSchemaName.getSchema());
            delayMonitorConfig.setTable(tableSchemaName.getName());
            delayMonitorConfig.setKey(getColumn(endpoint, GET_PRIMARY_KEY_COLUMN, tableSchemaName, false));
            delayMonitorConfig.setOnUpdate(getColumn(endpoint, GET_ON_UPDATE_COLUMN, tableSchemaName, false));
            delayMonitorConfigs.put(tableSchema, delayMonitorConfig);
        }
        return delayMonitorConfigs;
    }

    private static String getColumn(Endpoint endpoint, String getColumnSuffix, TableSchemaName tableSchemaName, Boolean removeSqlOperator) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        String sql = String.format(GET_COLUMN_PREFIX, tableSchemaName.getSchema(), tableSchemaName.getName()) + getColumnSuffix;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if (rs.next()) {
                return rs.getString(COLUMN_INDEX);
            }
        } catch (Throwable t) {
            logger.error("[[endpoint={}:{}]] getColumn({}) error: ", endpoint.getHost(), endpoint.getPort(), sql, t);
            removeSqlOperator(endpoint);
        } finally {
            if (readResource != null) {
                readResource.close();
            }
            if (removeSqlOperator) {
                removeSqlOperator(endpoint);
            }
        }
        return null;
    }

    /**
     * @param endpoint
     * @param tables tale without ``
     * @return key: database.table, value: createTblStmts
     */
    public static Map<String, String> getCreateTblStmts(Endpoint endpoint, List<String> tables, Boolean removeSqlOperator) {
        Map<String, String> stmts = Maps.newHashMap();
        for(String table : tables) {
            String createTblStmt = getCreateTblStmt(endpoint, TableSchemaName.getTableSchemaName(table), removeSqlOperator);
            String stmt = filterStmt(createTblStmt, endpoint, table);
            stmts.put(table, stmt);
        }
        return stmts;
    }
    
    public static String getCreateTblStmt(Endpoint endpoint,TableSchemaName table,Boolean removeSqlOperator) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        try {
            String sql = String.format(GET_CREATE_TABLE_STMT, table);
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if (rs.next()) {
                return rs.getString(CREATE_TABLE_INDEX);
            }
        } catch (Throwable t) {
            logger.error("[[monitor=table,endpoint={}:{}]] getCreateTblStmts error: ", endpoint.getHost(), endpoint.getPort(), t);
            removeSqlOperator(endpoint);
        } finally {
            if (readResource != null) {
                readResource.close();
            }
        }
        if (removeSqlOperator) {
            removeSqlOperator(endpoint);
        }
        return null;
    }

    // column use lowerCase
    public static Map<String, Set<String>> getAllColumnsByTable(Endpoint endpoint, List<TableSchemaName> tables, Boolean removeSqlOperator) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        Map<String, Set<String>> table2ColumnsMap = Maps.newHashMap();
        ReadResource readResource = null;
        for(TableSchemaName table : tables) {
            try {
                String sql = String.format(GET_ALL_COLUMN_PREFIX, table.getSchema(),table.getName());
                GeneralSingleExecution execution = new GeneralSingleExecution(sql);
                readResource = sqlOperatorWrapper.select(execution);
                ResultSet rs = readResource.getResultSet();
                int index = 1;
                HashSet<String> columns = Sets.newHashSet();
                if (rs.next()) {
                    final String[] columnNames = rs.getString(1).split(",");
                    for (String columnName : columnNames) {
                        // column case insensitive
                        columns.add(columnName.toLowerCase());
                    }
                }
                table2ColumnsMap.put(table.getDirectSchemaTableName(),columns);
            } catch (Throwable t) {
                logger.error("[[monitor=table,endpoint={}:{}]] getAllColumns error: ", endpoint.getHost(), endpoint.getPort(), t);
                removeSqlOperator(endpoint);
            } finally {
                if (readResource != null) {
                    readResource.close();
                }
            }
        }
        if (removeSqlOperator) {
            removeSqlOperator(endpoint);
        }
        return table2ColumnsMap;
    }

    public static Set<String> getAllCommonColumns(Endpoint endpoint,AviatorRegexFilter aviatorRegexFilter) {
        List<TableSchemaName> tablesAfterFilter = getTablesAfterRegexFilter(endpoint,aviatorRegexFilter);
        Map<String, Set<String>> allColumnsByTable = getAllColumnsByTable(endpoint, tablesAfterFilter, false);
        HashSet<String> commonColumns = Sets.newHashSet();
        for (Set<String> columns : allColumnsByTable.values()) {
            if (commonColumns.isEmpty()) {
                commonColumns.addAll(columns);
            } else {
                commonColumns.retainAll(columns);
                if (commonColumns.isEmpty()) {
                    break;
                }
            }
        }
        return commonColumns;
    }

    protected static String filterStmt(String roughStmt, Endpoint endpoint, String table) {
        // lower case
        String lowerCaseStmt = roughStmt.toLowerCase();

        Map<String, String> regexReplacementMap = new HashMap<>();
        String tableAutoIncrementRegex = "\\s*auto_increment=[0-9]+";
        String annotationRegex = "/\\*[\\s\\S]*?\\*/";
        String mysqlFieldCommentRegex = "\\s*comment\\s*'[\\s\\S]*?'\\s*,";
        String mysqlTableCommentRegex = "\\s*comment\\s*=\\s*'[\\s\\S]*?'";
        regexReplacementMap.put(tableAutoIncrementRegex, "");
        regexReplacementMap.put(annotationRegex, "");
        regexReplacementMap.put(mysqlFieldCommentRegex, ",");
        regexReplacementMap.put(mysqlTableCommentRegex, "");

        String filteredStmt = filterStmtByRegex(lowerCaseStmt, regexReplacementMap, endpoint, table);

        return filteredStmt.replaceAll("\r|\n", "");
    }

    protected static String filterStmtByRegex(String stmt, Map<String, String> regexReplacementMap, Endpoint endpoint, String table) {
        for(Map.Entry<String, String> regexReplacement : regexReplacementMap.entrySet()) {
            String regex = regexReplacement.getKey();
            String replacement = regexReplacement.getValue();
            Pattern patten = Pattern.compile(regex);
            boolean matchFlag = true;
            while(matchFlag) {
                Matcher matcher = patten.matcher(stmt);
                if(matcher.find()) {
                    String filterString = matcher.group();
                    logger.info("[[monitor=tableConsistency,endpoint={}:{}]] filter String({}) for table {}", endpoint.getHost(), endpoint.getPort(), filterString, table);
                    stmt = stmt.replace(filterString, replacement);
                } else {
                    matchFlag = false;
                }
            }
        }
        return stmt;
    }

    /**
     * DRC MySQL dependency: every table should have a column which is TIMESTAMP with ON UPDATE timing on the millisecond, i.e., "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"
     */
    @Deprecated
    public static List<String> checkOnUpdate(Endpoint endpoint, List<String> dbNames) {
        List<String> tablesWithoutOnUpdate = Lists.newArrayList();
        Map<String, String> createTblStmts = getAllCreateStmts(endpoint, dbNames);
        for(Map.Entry<String, String> entry : createTblStmts.entrySet()) {
            logger.info("check on update {}", entry.getKey());
            if(!entry.getValue().toLowerCase().contains(ON_UPDATE)) {
                tablesWithoutOnUpdate.add(entry.getKey());
            }
        }
        return tablesWithoutOnUpdate;
    }
    
    @Deprecated
    public static List<String> checkOnUpdateKey(Endpoint endpoint) {
        List<TableSchemaName> tableSchemaNames = getDefaultTables(endpoint);
        List<String> tablesWithoutOnUpdateKey = Lists.newArrayList();
        for(TableSchemaName tableSchemaName : tableSchemaNames) {
            String onUpdateColumn = getColumn(endpoint, GET_ON_UPDATE_COLUMN, tableSchemaName, false);
            if(!isKey(endpoint, tableSchemaName, onUpdateColumn, false)) {
                tablesWithoutOnUpdateKey.add(tableSchemaName.toString());
            }
        }
        return tablesWithoutOnUpdateKey;
    }

    public static boolean isKey(Endpoint endpoint, TableSchemaName tableSchemaName, String column, Boolean removeSqlOperator) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        String sql = String.format("show index from %s", tableSchemaName.toString());
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            while(rs.next()) {
                String curColumnName = rs.getString("Column_name");
                if(column.equals(curColumnName)) {
                    return true;
                }
            }
        } catch(Throwable t) {
            logger.error("[[endpoint={}:{},table={},column={}]] isKey error: ", endpoint.getHost(), endpoint.getPort(), tableSchemaName.toString(), column, t);
            removeSqlOperator(endpoint);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
            if(removeSqlOperator) {
                removeSqlOperator(endpoint);
            }
        }
        return false;
    }

    /**
     * DRC MySQL dependency: every table needs to have at least one primary key or unique key
     */
    @Deprecated
    public static List<String> checkUniqOrPrimary(Endpoint endpoint, List<String> dbNames) {
        List<String> tablesWithoutPkAndUk = Lists.newArrayList();
        Map<String, String> createTblStmts = getAllCreateStmts(endpoint, dbNames);
        for(Map.Entry<String, String> entry : createTblStmts.entrySet()) {
            logger.info("check pk uk {}", entry.getKey());
            logger.info("create pk uk table stmt: {}", entry.getValue());
            if(!entry.getValue().toLowerCase().contains(PRIMARY_KEY) && !entry.getValue().toLowerCase().contains(UNIQUE_KEY)) {
                tablesWithoutPkAndUk.add(entry.getKey());
            }
        }
        return tablesWithoutPkAndUk;
    }

    @Deprecated
    public static String checkMySqlSetting(Endpoint endpoint, String sql) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                return rs.getString(1);
            }
        } catch (Throwable t) {
            logger.error("[[endpoint={}:{}]]checkGtidMode error: ", endpoint.getHost(), endpoint.getPort(), t);
            removeSqlOperator(endpoint);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        return null;
    }
    
    
    public static List<String> checkApprovedTruncateTableList(Endpoint endpoint,boolean removeSqlOperator) {
        List<TableSchemaName> tables = getTables(endpoint, GET_APPROVED_TRUNCATE_TABLES, removeSqlOperator);
        return tables.stream().map(TableSchemaName::toString).collect(Collectors.toList());
    }

    
    public static String getUuid(String ip, int port, String user, String password, boolean master) throws Exception {
        Endpoint endpoint = new MySqlEndpoint(ip, port, user, password, master);
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        String uuid = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(UUID_COMMAND);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                uuid = rs.getString(UUID_INDEX);
            }
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        return uuid;
    }

    @Deprecated
    public static String getGtidExecuted(Endpoint endpoint) throws SQLException {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        String gtidExecuted = "";
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(GTID_EXECUTED_COMMAND);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                gtidExecuted = rs.getString(GTID_EXECUTED_INDEX);
            }
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        return new GtidSet(gtidExecuted).toString();
    }
    
    public static String getUnionExecutedGtid(Endpoint endpoint) {
        return new ExecutedGtidQueryTask(endpoint).call();
    }

    public static void removeSqlOperator(Endpoint endpoint) {
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = sqlOperatorMapper.remove(endpoint);
        if (writeSqlOperatorWrapper != null) {
            try {
                writeSqlOperatorWrapper.stop();
                writeSqlOperatorWrapper.dispose();
            } catch (Exception e) {
                logger.error("[[monitor=tableConsistency,endpoint={}:{}]] MySqlUtils sqlOperatorWrapper stop and dispose: ", endpoint.getHost(), endpoint.getPort(), e);
            }
        }
    }

    private static WriteSqlOperatorWrapper getSqlOperatorWrapper(Endpoint endpoint) {
        if(sqlOperatorMapper.containsKey(endpoint)) {
            return sqlOperatorMapper.get(endpoint);
        } else {
            WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
            try {
                sqlOperatorWrapper.initialize();
                sqlOperatorWrapper.start();
            } catch (Exception e) {
                logger.error("[[db={}:{}]]ColumnUtils.sqlOperatorWrapper initialize error: ", endpoint.getHost(), endpoint.getPort(), e);
            }
            sqlOperatorMapper.put(endpoint, sqlOperatorWrapper);
            return sqlOperatorWrapper;
        }
    }

    protected static String convertListToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        if(null != list && list.size() > 0) {
            for(int i = 0; i < list.size(); ++i) {
                if(i == 0) {
                    sb.append("'").append(list.get(i)).append("'");
                } else {
                    sb.append(",").append("'").append(list.get(i)).append("'");
                }
            }
        }
        return sb.toString();
    }

    private static Map<String, String> getAllCreateStmts(Endpoint endpoint, List<String> dbNames) {
        String sql;
        if(null == dbNames || dbNames.size() == 0) {
            sql = GET_DEFAULT_TABLES;
        } else {
            sql = String.format(GET_DB_TABLES_PREFIX, convertListToString(dbNames)) + GET_DB_TABLES_SUFFIX;
        }

        List<TableSchemaName> tables = getTables(endpoint, sql, false);
        return getCreateTblStmts(endpoint, tables.stream().map(TableSchemaName::getDirectSchemaTableName).collect(Collectors.toList()), false);
    }

    @Deprecated
    public static String getExecutedGtid(Endpoint endpoint) {
        return  getSqlResultString(endpoint, GTID_EXECUTED_COMMAND_V2, GTID_EXECUTED_INDEX_V2);
    }
    
    public static String getSqlResultString(Endpoint endpoint, String sql,int index) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                return rs.getString(index);
            }
        } catch (Throwable t) {
            logger.error("[[endpoint={}:{}]] sql:{} error: ", endpoint.getHost(), endpoint.getPort(),sql, t);
            removeSqlOperator(endpoint);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        return null;
    }

    public static Integer getSqlResultInteger(Endpoint endpoint, String sql,int index) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(sql);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                return rs.getInt(index);
            }
        } catch (Throwable t) {
            logger.error("[[endpoint={}:{}]] sql:{} error: ", endpoint.getHost(), endpoint.getPort(),sql, t);
            removeSqlOperator(endpoint);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        return null;
    }
    
    public static String checkBinlogMode(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkBinlogMode", endpoint.getSocketAddress());
        return getSqlResultString(endpoint,CHECK_BINLOG,SHOW_CERTAIN_VARIABLES_INDEX);
    }

    public static String checkBinlogFormat(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkBinlogFormat", endpoint.getSocketAddress());
        return getSqlResultString(endpoint,CHECK_BINLOG_FORMAT,SHOW_CERTAIN_VARIABLES_INDEX);
    }

    public static String checkBinlogVersion(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkBinlogVersion", endpoint.getSocketAddress());
        return getSqlResultString(endpoint,CHECK_BINLOG_VERSION1,SHOW_CERTAIN_VARIABLES_INDEX);
    }

    public static Integer checkAutoIncrementStep(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkAutoIncrementStep", endpoint.getSocketAddress());
        return getSqlResultInteger(endpoint, CHECK_INCREMENT_STEP,SHOW_CERTAIN_VARIABLES_INDEX);
    }

    public static Integer checkAutoIncrementOffset(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkAutoIncrementOffset ", endpoint.getSocketAddress());
        return getSqlResultInteger(endpoint, CHECK_INCREMENT_OFFSET,SHOW_CERTAIN_VARIABLES_INDEX);
    }

    public static Integer checkDrcTables(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] checkDrcTables ", endpoint.getSocketAddress());
        return getSqlResultInteger(endpoint, CHECK_DRC_TABLES,1);
    }

    public static String checkGtidMode(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] check gtid mode", endpoint.getSocketAddress());
        return getSqlResultString(endpoint, CHECK_GTID_MODE,1);
    }

    public static String checkBinlogTransactionDependency(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] check writeset", endpoint.getSocketAddress());
        return getSqlResultString(endpoint, CHECK_BINLOG_TRANSACTION_DEPENDENCY_TRACKING,1);
    }
    
    public static Integer checkBtdhs(Endpoint endpoint) {
        logger.info("[[tag=preCheck,endpoint={}]] check btdhs", endpoint.getSocketAddress());
        return getSqlResultInteger(endpoint,BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE,BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_INDEX);
    }
    
    public static List<TableCheckVo> checkTablesWithFilter(Endpoint endpoint,String nameFilter) {
        List<TableCheckVo> checkTableVos = Lists.newLinkedList();
        
        if(StringUtils.isEmpty(nameFilter)) {
            nameFilter = MATCH_ALL_FILTER;
        }
        List<TableSchemaName> tables = getTablesAfterRegexFilter(endpoint, new AviatorRegexFilter(nameFilter));
        HashSet<String> tablesApprovedTruncate = Sets.newHashSet(checkApprovedTruncateTableList(endpoint,false));
        for (TableSchemaName table : tables) {
            TableCheckVo tableVo = new TableCheckVo(table);
            String onUpdateColumn = getColumn(endpoint, GET_ON_UPDATE_COLUMN, table, false);
            if (StringUtils.isEmpty(onUpdateColumn)) {
                tableVo.setNoOnUpdateColumn(true);
                tableVo.setNoOnUpdateKey(true);
            } else {
                tableVo.setNoOnUpdateKey(!isKey(endpoint, table, onUpdateColumn, false));
            }
            String createTblStmt = getCreateTblStmt(endpoint, table, false);
            if (StringUtils.isEmpty(createTblStmt) || 
                    (!createTblStmt.toLowerCase().contains(PRIMARY_KEY) && !createTblStmt.toLowerCase().contains(UNIQUE_KEY))) {
                tableVo.setNoPkUk(true);
            }
            if (StringUtils.isEmpty(createTblStmt) || createTblStmt.toLowerCase().contains(DEFAULT_ZERO_TIME)) {
                tableVo.setTimeDefaultZero(true);
            }
            if (tablesApprovedTruncate.contains(tableVo.getFullName())) {
                tableVo.setApproveTruncate(true);
            }
            
            if (tableVo.hasProblem()) {
                checkTableVos.add(0,tableVo);
            } else {
                checkTableVos.add(tableVo);
            }
        }
        return checkTableVos;
    }

    public static boolean testAccount(Endpoint endpoint) {
        WriteSqlOperatorWrapper sqlOperatorWrapper = getSqlOperatorWrapper(endpoint);
        ReadResource readResource = null;
        try {
            GeneralSingleExecution execution = new GeneralSingleExecution(CHECK_ACCOUNT_AVAILABLE);
            readResource = sqlOperatorWrapper.select(execution);
            ResultSet rs = readResource.getResultSet();
            if(rs.next()) {
                return true;
            }
            return false;
        } catch (Throwable t) {
            logger.error("[[endpoint={}:{}]] sql error in check Account: ", endpoint.getHost(), endpoint.getPort(), t);
            removeSqlOperator(endpoint);
            return false;
        } finally {
            if(readResource != null) {
                readResource.close();
            }
            
        }
    }
    
    public static String checkAccounts(List<Endpoint> endpoints) {
        for (Endpoint endpoint : endpoints) {
            if (!testAccount(endpoint)) {
                return "three accounts might not be ready";
            }
        }
        return "three accounts ready";
    }


    public static final class TableSchemaName {
        private String schema;
        private String name;
        public TableSchemaName(String schema, String name) {
            this.schema = schema;
            this.name = name;
        }
       
        public static TableSchemaName getTableSchemaName(String schemaName) {
            if (StringUtils.isEmpty(schemaName) || !schemaName.contains(".")) {
                return null;
            }
            String[] split = schemaName.split("\\.");
            return new TableSchemaName(split[0],split[1]);
        }
        
        public String getSchema() { return schema; }
        public String getName() { return name; }
        
        @Override
        public String toString() {
            return String.format("`%s`.`%s`", schema, name);
        }

        public String getDirectSchemaTableName() {
            return String.format("%s.%s", schema, name);
        }

    }
}
