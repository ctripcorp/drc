package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.config.DynamicConfig;
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
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.ghost.DDLPredication;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.index.IndexExtractor;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbCreateTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbDisposeTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbDisposeTask.Result;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbRestoreTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.MySQLInstance;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.store.EventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Sets;
import com.wix.mysql.distribution.Version;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractColumns;
import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractValues;
import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTask.SHOW_TABLES_QUERY;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.*;
import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.getDefaultPoolProperties;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;
import static com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager.SchemaStatus.*;
import static ctrip.framework.drc.mysql.EmbeddedDb.*;

/**
 * for in memory mysql
 *
 * @Author limingdong
 * @create 2020/3/2
 */
public class MySQLSchemaManager extends AbstractSchemaManager implements SchemaManager {

    public static final String INDEX_QUERY = "SELECT INDEX_NAME,COLUMN_NAME FROM information_schema.statistics WHERE `table_schema` = \"%s\" AND `table_name` = \"%s\" and NON_UNIQUE=0 ORDER BY SEQ_IN_INDEX;";

    private static final String INFORMATION_SCHEMA_QUERY = "select * from information_schema.COLUMNS where `TABLE_SCHEMA`=\"%s\" and `TABLE_NAME`=\"%s\" order by `ORDINAL_POSITION`";


    public static final int MAX_DB_START_PARALLEL = 5;

    private static ExecutorService notifyExecutorService = ThreadUtils.newFixedThreadPool(MAX_DB_START_PARALLEL, "Db-Start");


    private static final int SOCKET_TIMEOUT = 60000;


    private MySQLInstance embeddedDb;
    protected Version embeddedDbVersion;

    private TransactionCache transactionCache;

    protected EventStore eventStore;

    private SchemaStatus schemaStatus = Uninitialized;

    public MySQLSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName);
        this.baseEndpointEntity = baseEndpointEntity;
    }

    @Override
    protected void doInitialize() {
        Future<MySQLInstance> dbResult = notifyExecutorService.submit(() -> {
            inMemoryEndpoint = new DefaultEndPoint(host, port, user, password);
            PoolProperties poolProperties = getDefaultPoolProperties(inMemoryEndpoint);
            String timeout = String.format("connectTimeout=%s;socketTimeout=%s", CONNECTION_TIMEOUT, SOCKET_TIMEOUT);
            poolProperties.setConnectionProperties(timeout);
            inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint, poolProperties);
            embeddedDbVersion = Version.v8_0_32;

            // kill existing mysql process if version not match
            Result result = new RetryTask<>(new DbDisposeTask(port, registryKey, embeddedDbVersion)).call();
            if (result == Result.FAIL) {
                throw new DrcServerException(String.format("[EmbeddedDb] dispose error for %s", registryKey));
            }

            if (isUsed(port)) {
                logger.info("start restore db for {}:{}", registryKey, port);
                return new RetryTask<>(new DbRestoreTask(port, registryKey, embeddedDbVersion)).call();
            } else {
                logger.info("start create db for {}:{}", registryKey, port);
                return new RetryTask<>(new DbCreateTask(port, registryKey, embeddedDbVersion)).call();
            }
        });
        try {
            embeddedDb = dbResult.get();
        } catch (Throwable e) {
            throw new DrcServerException("MySQLSchemaManager initialize error", e);
        }
        if (embeddedDb == null) {
            throw new DrcServerException(String.format("[EmbeddedDb] init error for %s", registryKey));
        }

        this.afterStart();
    }

    private void afterStart() {
        if (embeddedDbVersion == Version.v8_0_32) {
            setDefaultCollationForUtf8mb4();
        }
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private void setDefaultCollationForUtf8mb4() {
        try (Connection connection = inMemoryDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("set session default_collation_for_utf8mb4=utf8mb4_general_ci;");
            statement.execute("set global default_collation_for_utf8mb4=utf8mb4_general_ci;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableInfo find(String schema, String table) {
        TableId keys = new TableId(toLowerCaseIfNotNull(schema), toLowerCaseIfNotNull(table));
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
    public synchronized Map<String, Map<String, String>> snapshot() {
        if (snapshotCacheNeedInit() || DynamicConfig.getInstance().getDisableSnapshotCacheSwitch()) {
            Map<String, Map<String, String>> snapshot = doSnapshot(inMemoryEndpoint);
            if (!CollectionUtils.isEmpty(schemaCache)) {
                boolean same = schemaCache.equals(snapshot);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.ddl.snapshot.cache.compare." + same, registryKey);
            }
            schemaCache = snapshot;
            tableInfoMap.clear();
        }
        return Collections.unmodifiableMap(schemaCache);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public boolean isEmbeddedDbEmpty() {
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(SHOW_DATABASES_QUERY)) {
                    List<String> databases = extractValues(resultSet, null);
                    return EXCLUDED_DB.containsAll(databases);
                }
            }
        } catch (SQLException e) {
            DDL_LOGGER.error("queryDb for {} error", registryKey, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * clear first, then apply
     */
    @Override
    public boolean clone(Endpoint endpoint) {
        Map<String, Map<String, String>> ddlSchemas = doSnapshot(endpoint);
        boolean res = doClone(ddlSchemas);
        DDL_LOGGER.info("[Dump] remote table info finished");
        return res;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public TableInfo queryTableInfoByIS(DataSource dataSource, String schema, String table) {
        String query = String.format(INFORMATION_SCHEMA_QUERY, schema, table);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    TableInfo tableInfo = extractColumns(resultSet, embeddedDbVersion == Version.v8_0_32);
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
                tableMapLogEvent.releaseMergedByteBufs();
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
        schemaCache.clear();
        inMemoryDataSource.close(true);
        embeddedDb.destroy();
        ddlMonitorExecutorService.shutdown();
    }

    @Override
    public synchronized void update(Object args, Observable observable) {
        if (observable instanceof ConnectionObservable && schemaStatus == Uninitialized) {
            if(initEmbeddedMySQL()) {
                schemaStatus = Initialized;
                DDL_LOGGER.info("[SchemaStatus] set to Initialized for {}", registryKey);
            }
        }

    }

    private boolean initEmbeddedMySQL() {
        if (shouldInitEmbeddedMySQL()) {
            schemaStatus = Initialing;
            DDL_LOGGER.info("[SchemaStatus] set to Initialing for {}", registryKey);
            return DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.replicator.schema.remote.dump", registryKey, () -> MySQLSchemaManager.this.clone(endpoint));
        }
        return true;
    }

    @VisibleForTesting
    protected static String toLowerCaseIfNotNull(String s) {
        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

    public void setEventStore(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    @Override
    protected boolean shouldInitEmbeddedMySQL() {
        FileManager fileManager = eventStore.getFileManager();
        File logDir = fileManager.getDataDir();
        File[] files = logDir.listFiles();
        return files == null || files.length == 0;
    }

    @Override
    public boolean shouldRecover(boolean fromLatestLocalBinlog) {
        boolean dbEmpty = isEmbeddedDbEmpty();
        if (!dbEmpty) {
            DDL_LOGGER.info("[Recovery Skip] due to already init for {} (Version: {})", registryKey, embeddedDbVersion);
        } else {
            DefaultEventMonitorHolder.getInstance().logEvent("drc.current.schema.empty", registryKey);
        }
        return dbEmpty;
    }

    enum SchemaStatus {

        Uninitialized,

        Initialing,

        Initialized,

    }
}
