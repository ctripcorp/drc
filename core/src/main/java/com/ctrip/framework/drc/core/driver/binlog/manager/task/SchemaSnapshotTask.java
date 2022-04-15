package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor.extractValues;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.EXCLUDED_DB;
import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.SHOW_DATABASES_QUERY;

/**
 * @Author limingdong
 * @create 2022/4/15
 */
public class SchemaSnapshotTask extends AbstractSchemaTask<Map<String, Map<String, String>>> implements NamedCallable<Map<String, Map<String, String>>> {

    public static final String SHOW_CREATE_TABLE_QUERY = "show create table `%s`.`%s`;";

    public static final String SHOW_TABLES_QUERY = "show full tables from `%s` where Table_type = 'BASE TABLE';";

    public SchemaSnapshotTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
    }

    @Override
    public Map<String, Map<String, String>> call() throws SQLException {
        Map<String, Map<String, String>> res = Maps.newConcurrentMap();
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {

                ResultSet resultSet = statement.executeQuery(SHOW_DATABASES_QUERY);
                List<String> databases = extractValues(resultSet, null);
                for (String schema : databases) {
                    if (EXCLUDED_DB.contains(schema)) {
                        continue;
                    }

                    Map<String, String> createTables = Maps.newHashMap();
                    resultSet = statement.executeQuery(String.format(SHOW_TABLES_QUERY, schema));
                    List<String> tables = extractValues(resultSet, "BASE TABLE");

                    for (String table : tables) {

                        String createSql = String.format(SHOW_CREATE_TABLE_QUERY, schema, table);
                        resultSet = statement.executeQuery(createSql);

                        while (resultSet.next()) {
                            String createTable = resultSet.getString(2);
                            createTable = createTable.replaceFirst("`" + table + "`", "`" + schema + "`.`" + table + "`");
                            createTables.put(table, createTable);
                        }
                    }

                    res.put(schema, createTables);
                }
                resultSet.close();
            }
        } catch (SQLException e) {
            DDL_LOGGER.error("snapshot error", e);
            throw e;
        }

        return res;
    }

}
