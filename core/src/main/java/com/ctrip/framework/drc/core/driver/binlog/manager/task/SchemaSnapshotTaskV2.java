package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.unidal.tuple.Triple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractValues;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.EXCLUDED_DB;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.SHOW_DATABASES_QUERY;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * SchemaSnapshotTask concurrent version
 */
public class SchemaSnapshotTaskV2 extends AbstractSchemaTask<Map<String, Map<String, String>>> implements NamedCallable<Map<String, Map<String, String>>> {

    private final ExecutorService service = ThreadUtils.newCachedThreadPool("SchemaSnapshotTaskV2");

    public static final String SHOW_CREATE_TABLE_QUERY = "show create table `%s`.`%s`;";

    public static final String SHOW_TABLES_QUERY = "show full tables from `%s` where Table_type = 'BASE TABLE';";
    private final String registryKey;

    public SchemaSnapshotTaskV2(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource, String registryKey) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.registryKey = registryKey;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public Map<String, Map<String, String>> call() throws SQLException {
        Map<String, Map<String, String>> res = Maps.newConcurrentMap();
        List<Callable<Triple<String, String, String>>> allTasks = Lists.newArrayList();
        try (Connection connection = inMemoryDataSource.getConnection();
             Statement statement = connection.createStatement()) {

            List<String> databases;
            try (ResultSet resultSet = statement.executeQuery(SHOW_DATABASES_QUERY)) {
                databases = extractValues(resultSet, null);
            }
            for (String schema : databases) {
                if (EXCLUDED_DB.contains(schema)) {
                    continue;
                }
                try (ResultSet resultSet = statement.executeQuery(String.format(SHOW_TABLES_QUERY, schema));) {
                    List<String> tables = extractValues(resultSet, "BASE TABLE");
                    res.put(schema, new HashMap<>(tables.size()));

                    for (String table : tables) {
                        allTasks.add(new CreateTableQueryTask(inMemoryDataSource, schema, table));
                    }
                }
            }
            int concurrency = DynamicConfig.getInstance().getSnapshotTaskConcurrency();
            DDL_LOGGER.info("[SchemaSnapshotTaskV2] the concurrency of is: {}, task num: {}", concurrency, allTasks.size());
            try {
                List<List<Callable<Triple<String, String, String>>>> taskPartitions = Lists.partition(allTasks, concurrency);
                for (List<Callable<Triple<String, String, String>>> subTasks : taskPartitions) {
                    List<Future<Triple<String, String, String>>> futures = service.invokeAll(subTasks);
                    for (Future<Triple<String, String, String>> future : futures) {
                        Triple<String, String, String> result = future.get(60, TimeUnit.SECONDS);
                        if (result == null || StringUtils.isEmpty(result.getLast())) {
                            continue;
                        }
                        String schema = result.getFirst();
                        String table = result.getMiddle();
                        String createTable = result.getLast();
                        res.get(schema).put(table, createTable);
                    }
                }
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                DDL_LOGGER.error("snapshot v2 error", e);
                throw new RuntimeException(e);
            }
            return res;
        } catch (SQLException e) {
            DDL_LOGGER.error("snapshot error", e);
            throw e;
        }
    }
    @Override
    public void afterException(Throwable t) {
        super.afterException(t);
        DDL_LOGGER.error("snapshot fail for {}  {}", registryKey, t);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.ddl.snapshot.failed", String.format("%s\nEXCEPTION:%s", String.join(".", registryKey), t.getCause()));
    }
    public static class CreateTableQueryTask implements NamedCallable<Triple<String, String, String>> {

        private String schema;
        private String table;
        private DataSource dataSource;

        public CreateTableQueryTask(DataSource dataSource, String schema, String table) {
            this.dataSource = dataSource;
            this.schema = schema;
            this.table = table;
        }

        @Override
        @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
        public Triple<String, String, String> call() throws SQLException {
            String createSql = String.format(SHOW_CREATE_TABLE_QUERY, schema, table);
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(createSql)) {
                if (resultSet.next()) {
                    String createTable = resultSet.getString(2);
                    return new Triple<>(schema, table, createTable.replaceFirst("`" + table + "`", "`" + schema + "`.`" + table + "`"));
                }
                return new Triple<>(schema, table, null);
            } catch (SQLException e) {
                String message = e.getMessage();
                if (message.contains(String.format("Table '%s.%s' doesn't exist", schema, table))
                        || message.contains(String.format("Unknown database '%s'", schema))) {
                    return new Triple<>(schema, table, null);
                }
                throw e;
            }
        }
        @Override
        public void afterException(Throwable t) {
            DDL_LOGGER.error("show create table fail for {} {} {}", schema, table, t);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.ddl.table.show.failed", String.format("%s\nEXCEPTION:%s", String.join(".", schema, table), t.getCause()));
        }
    }

}
