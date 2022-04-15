package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.ConnectionObservable;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcDdlLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.AbstractSchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.exception.DrcServerException;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.index.IndexExtractor;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbInitTask;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import com.wix.mysql.EmbeddedMysql;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractColumns;
import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractValues;
import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTask.SHOW_TABLES_QUERY;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.EXCLUDED_DB;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.SHOW_DATABASES_QUERY;
import static ctrip.framework.drc.mysql.EmbeddedDb.*;

/**
 * for in memory mysql
 *
 * @Author limingdong
 * @create 2020/3/2
 */
public class MySQLSchemaManager extends AbstractSchemaManager implements SchemaManager {

    protected static final Logger DDL_LOGGER = LoggerFactory.getLogger("com.ctrip.framework.drc.replicator.impl.inbound.filter.DdlFilter");

    public static final String INDEX_QUERY = "SELECT INDEX_NAME,COLUMN_NAME FROM information_schema.statistics WHERE `table_schema` = \"%s\" AND `table_name` = \"%s\" and NON_UNIQUE=0 ORDER BY SEQ_IN_INDEX;";

    private static final String INFORMATION_SCHEMA_QUERY = "select * from information_schema.COLUMNS where `TABLE_SCHEMA`=\"%s\" and `TABLE_NAME`=\"%s\"";

    private AtomicReference<Map<String, Map<String, String>>> schemaCache = new AtomicReference<>();

    public static final int PORT_STEP = 10000;

    private boolean integrityTest = "true".equalsIgnoreCase(System.getProperty(SystemConfig.REPLICATOR_WHITE_LIST));

    private boolean firstTimeIntegrityTest = false;

    private EmbeddedMysql embeddedDb;

    private TransactionCache transactionCache;

    protected EventStore eventStore;

    public MySQLSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName);
        this.baseEndpointEntity = baseEndpointEntity;
        logger.info("[Schema] port is {}", port);
    }

    @Override
    protected void doInitialize() {
        embeddedDb = new RetryTask<>(new DbInitTask(port)).call();
        if (embeddedDb == null) {
            throw new DrcServerException(String.format("[EmbeddedDb] init error for %s", clusterName));
        }
        inMemoryEndpoint = new DefaultEndPoint(host, port, user, password);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
    }

    @Override
    public TableInfo find(String schema, String table) {
        TableId keys = new TableId(schema, table);
        TableInfo tableMeta = tableInfoMap.get(keys);
        if (tableMeta == null) {
            synchronized (this) {
                tableMeta = tableInfoMap.get(keys);
                if (tableMeta == null) {
                    tableMeta = queryTableInfoByIS(inMemoryDataSource, schema, table);

                    if (tableMeta != null) {
                        tableMeta.setDbName(schema);
                        tableMeta.setTableName(table);
                        tableInfoMap.put(keys, tableMeta);
                    }
                }
            }
        }

        return tableMeta;
    }

    @Override
    public Map<String, Map<String, String>> snapshot() {
        Map<String, Map<String, String>> snapshot = doSnapshot(inMemoryEndpoint);
        if (!CollectionUtils.isEmpty(snapshot)) {
            schemaCache.set(snapshot);
        } else {
            DefaultEventMonitorHolder.getInstance().logEvent("Drc.replicator.schema.snapshot", "Blank");
            if (schemaCache.get() != null) {
                return schemaCache.get();
            }
            return snapshot;
        }
        return snapshot;
    }

    /**
     * clear first, then apply
     */
    @Override
    public void clone(Endpoint endpoint) {
        Map<String, Map<String, String>> ddlSchemas = doSnapshot(endpoint);
        doClone(ddlSchemas);
        DDL_LOGGER.info("[Dump] remote table info finished");
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public TableInfo queryTableInfoByIS(DataSource dataSource, String schema, String table) {
        String query = String.format(INFORMATION_SCHEMA_QUERY, schema, table);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    TableInfo tableInfo = extractColumns(resultSet);
                    String indexQuery = String.format(INDEX_QUERY, schema, table);
                    try (ResultSet indexResultSet = statement.executeQuery(indexQuery)) {
                        List<List<String>> indexes = IndexExtractor.extractIndex(indexResultSet);
                        tableInfo.setIdentifiers(indexes);
                    }
                    return tableInfo;
                }
            }
        } catch (SQLException e) {
            DDL_LOGGER.error("queryTableInfoByIS for {}.{} error", schema, table, e);
        }

        return null;
    }

    /**
     * drc_ddl_log_event
     * @param queryString
     */
    @Override
    public void persistDdl(String dbName, String tableName, String queryString) {
        if (StringUtils.isBlank(dbName)) {
            dbName = StringUtils.EMPTY;
        }
        if (StringUtils.isBlank(tableName)) {
            tableName = StringUtils.EMPTY;
        }
        try {
            if (StringUtils.isNotBlank(queryString)) {
                DrcDdlLogEvent ddlLogEvent = new DrcDdlLogEvent(dbName, queryString, 0, 0);
                transactionCache.add(ddlLogEvent);
            }
        } catch (IOException e) {
            DDL_LOGGER.error("[Write] DRC ddl event error", e);
        }
        DDL_LOGGER.info("[Persist] drc ddl event for {}.{}", dbName, tableName);
    }

    /**
     * drc_table_map_log_event
     * @param tableInfo
     */
    @Override
    public void persistColumnInfo(TableInfo tableInfo, boolean writeDirect) {
        List<TableMapLogEvent.Column> columnList = tableInfo.getColumnList();
        if (columnList == null || columnList.isEmpty()) {
            DDL_LOGGER.info("[Skip] persist drc table map log event for {}.{}", tableInfo.getDbName(), tableInfo.getTableName());
            return;
        }
        String tableName = tableInfo.getTableName();
        if (StringUtils.isNotBlank(tableName) && DDLPredication.mayGhostOps(tableName)) {
            DDL_LOGGER.info("[Skip] persist ghost drc table map log event for {}.{}", tableInfo.getDbName(), tableInfo.getTableName());
            return;
        }
        try {
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(0, 0, 0, tableInfo.getDbName().toLowerCase(), tableInfo.getTableName().toLowerCase(), columnList, tableInfo.getIdentifiers());
            if (writeDirect) {
                tableMapLogEvent.write(eventStore);
                tableMapLogEvent.release();
            } else {
                transactionCache.add(tableMapLogEvent);
            }
        } catch (IOException e) {
            DDL_LOGGER.error("[Write] DRC TableMapLogEvent error", e);
        }
        DDL_LOGGER.info("[Persist] drc table map log event for {}.{}", tableInfo.getDbName(), tableInfo.getTableName());
    }

    @VisibleForTesting
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Set<TableId> getTableIds(DataSource dataSource) {
        Set<TableId> tableIds = Sets.newHashSet();
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet dResultSet = statement.executeQuery(SHOW_DATABASES_QUERY)) {
                    List<String> databases = extractValues(dResultSet, null);
                    for (String schema : databases) {
                        if (EXCLUDED_DB.contains(schema)) {
                            continue;
                        }

                        try (ResultSet bResultSet = statement.executeQuery(String.format(SHOW_TABLES_QUERY, schema))) {
                            List<String> tables = extractValues(bResultSet, "BASE TABLE");

                            if (tables.isEmpty()) {
                                tableIds.add(new TableId(schema, null));
                                continue;
                            }

                            for (String table : tables) {
                                tableIds.add(new TableId(schema, table));
                            }
                        }

                    }
                }
            }
        } catch (SQLException e) {
            DDL_LOGGER.error("snapshot error", e);
        }

        return tableIds;
    }

    @Override
    protected void doDispose() {
        tableInfoMap.clear();
        inMemoryDataSource.close(true);
        embeddedDb.stop();
        ddlMonitorExecutorService.shutdown();
    }

    @Override
    public void update(Object args, Observable observable) {
        if (observable instanceof ConnectionObservable) {
            initEmbeddedMySQL();
        }

    }

    private void initEmbeddedMySQL() {
        if (integrityTest && !firstTimeIntegrityTest) {
            firstTimeIntegrityTest = true;
            return;
        }
        FileManager fileManager = eventStore.getFileManager();
        File logDir = fileManager.getDataDir();
        File[] files = logDir.listFiles();
        if (files == null || files.length == 0) {
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.replicator.schema.remote.dump", clusterName, new Task() {
                @Override
                public void go() {
                    MySQLSchemaManager.this.clone(endpoint);
                }
            });
        }
    }

    public void setEventStore(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }
}
