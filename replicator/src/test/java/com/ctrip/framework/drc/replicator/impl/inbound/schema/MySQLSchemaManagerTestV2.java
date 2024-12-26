package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTaskV2.SHOW_CREATE_TABLE_QUERY;
import static com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTaskV2.SHOW_TABLES_QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author yongnian
 * @create 2024/12/25 14:53
 */
public class MySQLSchemaManagerTestV2 {
    @InjectMocks
    MySQLSchemaManager mySQLSchemaManager = new MySQLSchemaManager(mock(Endpoint.class), 0, "", mock(BaseEndpointEntity.class));

    @Mock
    protected DataSource inMemoryDataSource;

    @Mock
    protected Endpoint inMemoryEndpoint;


    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void test() throws Exception {
        String createTableSql = "create table xxx;";
        String dbName = "db1";
        String tableName = "table1";

        try (MockedStatic<DataSourceManager> theMock = Mockito.mockStatic(DataSourceManager.class)) {
            mockSnapshotResult(theMock, dbName, tableName, createTableSql);

            // test init bug
            Assert.assertTrue(mySQLSchemaManager.isSnapshotCacheNotInit());
            // 1. mock doSnapshot remote， not init
            Endpoint remoteEndpoint = inMemoryEndpoint;
            Map<String, Map<String, String>> stringMapMap = mySQLSchemaManager.doSnapshot(remoteEndpoint);
            Assert.assertFalse(stringMapMap.isEmpty());
            Assert.assertEquals(createTableSql, stringMapMap.get(dbName).get(tableName));
            Assert.assertTrue(mySQLSchemaManager.isSnapshotCacheNotInit());

            // 2. mock ddl， not init
            mockSnapshotResult(theMock, dbName, tableName, createTableSql);
            List<TableId> tableIds = new ArrayList<>();
            tableIds.add(new TableId("db1", "table1"));
            mySQLSchemaManager.refresh(tableIds);
            Assert.assertTrue(mySQLSchemaManager.isSnapshotCacheNotInit());
        }


        try (MockedStatic<DataSourceManager> theMock = Mockito.mockStatic(DataSourceManager.class)) {
            mockSnapshotResult(theMock, dbName, tableName, createTableSql);

            // 3. mock local snapshot, init
            Assert.assertTrue(mySQLSchemaManager.isSnapshotCacheNotInit());
            mySQLSchemaManager.snapshot();
            Assert.assertFalse(mySQLSchemaManager.isSnapshotCacheNotInit());
        }


    }

    private void mockSnapshotResult(MockedStatic<DataSourceManager> theMock, String dbName, String tableName, String createTableSql) throws SQLException {
        DataSource mockDatasource;
        DataSourceManager mockDatasourceManager = mock(DataSourceManager.class);
        theMock.when(DataSourceManager::getInstance).thenReturn(mockDatasourceManager);

        mockDatasource = mock(DataSource.class);
        when(mockDatasourceManager.getDataSource(any())).thenReturn(mockDatasource);

        mockDataSource(dbName, tableName, createTableSql, mockDatasource);
        mockDataSource(dbName, tableName, createTableSql, inMemoryDataSource);
    }

    private static void mockDataSource(String dbName, String tableName, String createTableSql, DataSource mockDatasource) throws SQLException {
        Connection mockConnection = mock(Connection.class);
        when(mockDatasource.getConnection()).thenReturn(mockConnection);
        ResultSet mockShowDatabaseQuery = mock(ResultSet.class);

        // show dbs
        Statement mockStatement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(MySQLConstants.SHOW_DATABASES_QUERY)).thenReturn(mockShowDatabaseQuery);
        when(mockShowDatabaseQuery.next()).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);

        when(mockShowDatabaseQuery.getString(1)).thenReturn(dbName);

        // show tables
        ResultSet mockShowTablesQuery = mock(ResultSet.class);
        when(mockShowTablesQuery.next()).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
        when(mockShowTablesQuery.getString(1)).thenReturn(tableName);
        when(mockStatement.executeQuery(String.format(SHOW_TABLES_QUERY, dbName))).thenReturn(mockShowTablesQuery);

        // show create table
        ResultSet mockShowCreateTable = mock(ResultSet.class);
        when(mockShowCreateTable.next()).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
        when(mockShowCreateTable.getString(2)).thenReturn(createTableSql).thenReturn(createTableSql);
        when(mockStatement.executeQuery(String.format(SHOW_CREATE_TABLE_QUERY, dbName, tableName))).thenReturn(mockShowCreateTable).thenReturn(mockShowCreateTable);
    }
}
