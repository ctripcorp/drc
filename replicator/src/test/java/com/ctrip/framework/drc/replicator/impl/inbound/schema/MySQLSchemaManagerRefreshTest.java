package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.AbstractSchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableId;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.replicator.AllTests;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidConfig;
import com.ctrip.framework.drc.replicator.container.zookeeper.UuidOperator;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStore;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;

public class MySQLSchemaManagerRefreshTest extends MockTest {

    @Mock
    private Filter<ITransactionEvent> filterChain;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Set<UUID> uuids = Sets.newHashSet();
    private static final String DB_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final int APPLIER_PORT = 8385;

    private DataSource remoteDataSource;

    private Endpoint endpoint = new DefaultEndPoint(AllTests.SRC_IP, AllTests.SRC_PORT, AllTests.MYSQL_USER, AllTests.MYSQL_PASSWORD);

    private MySQLSchemaManager mySQLSchemaManager;

    private FileManager fileManager;

    private FilePersistenceEventStore filePersistenceEventStore;

    private TransactionCache transactionCache;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        when(replicatorConfig.getWhiteUUID()).thenReturn(uuids);
        when(replicatorConfig.getRegistryKey()).thenReturn("");
        when(uuidOperator.getUuids(anyString())).thenReturn(uuidConfig);
        when(uuidConfig.getUuids()).thenReturn(Sets.newHashSet("c372080a-1804-11ea-8add-98039bbedf9c"));

        remoteDataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        mySQLSchemaManager = new MySQLSchemaManager(endpoint, APPLIER_PORT + 2000, "test", null);

        filePersistenceEventStore = new FilePersistenceEventStore(mySQLSchemaManager, uuidOperator, replicatorConfig);
        mySQLSchemaManager.setEventStore(filePersistenceEventStore);
        transactionCache = new EventTransactionCache(filePersistenceEventStore, filterChain);
        mySQLSchemaManager.setTransactionCache(transactionCache);
        fileManager = filePersistenceEventStore.getFileManager();

        transactionCache.initialize();
        mySQLSchemaManager.initialize();
        filePersistenceEventStore.initialize();

        File file = fileManager.getDataDir();
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
        }
        transactionCache.start();
        mySQLSchemaManager.start();
        filePersistenceEventStore.start();
    }

    @Test
    public void testRefresh() throws IllegalAccessException, NoSuchFieldException {
        String db1 = "db1";
        String table1 = "table1";
        TableId tableId = new TableId(db1, table1);
        Map<TableId, TableInfo> tableIdTableInfoMap = getTableIdTableInfoMap();


        Map<String, Map<String, String>> schemaCache = getSchemaCache();
        Assert.assertNull(schemaCache.computeIfAbsent(db1, (e) -> new HashMap<>()).get(table1));
        Assert.assertTrue(tableIdTableInfoMap.get(tableId) == null || CollectionUtils.isEmpty(tableIdTableInfoMap.get(tableId).getColumnList()));

        List<TableId> list = Lists.newArrayList(tableId);
        String sql = String.format(DB_TABLE_TEMPLATE, db1, table1);
        mySQLSchemaManager.apply(db1, table1, "create database if not exists db1", QueryType.QUERY, null);
        mySQLSchemaManager.apply(db1, table1, sql, QueryType.CREATE, null);
        Assert.assertNull(schemaCache.computeIfAbsent(db1, (e) -> new HashMap<>()).get(table1));
        Assert.assertTrue(tableIdTableInfoMap.get(tableId) == null || CollectionUtils.isEmpty(tableIdTableInfoMap.get(tableId).getColumnList()));

        mySQLSchemaManager.refresh(list);
        Assert.assertNotNull(schemaCache.get(db1).get(table1));
        Assert.assertTrue(tableIdTableInfoMap.get(tableId) == null || CollectionUtils.isEmpty(tableIdTableInfoMap.get(tableId).getColumnList()));

        mySQLSchemaManager.find(db1, table1);
        Map<String, Map<String, String>> snapshot = mySQLSchemaManager.snapshot();
        Assert.assertFalse(tableIdTableInfoMap.get(tableId) == null || CollectionUtils.isEmpty(tableIdTableInfoMap.get(tableId).getColumnList()));

    }

    private Map<TableId, TableInfo> getTableIdTableInfoMap() throws NoSuchFieldException, IllegalAccessException {
        Field tableInfoMap1 = AbstractSchemaManager.class.getDeclaredField("tableInfoMap");
        tableInfoMap1.setAccessible(true);
        Map<TableId, TableInfo> tableInfoMap = ((Map<TableId, TableInfo>) tableInfoMap1.get(mySQLSchemaManager));
        return tableInfoMap;
    }

    private Map<String, Map<String, String>> getSchemaCache() throws NoSuchFieldException, IllegalAccessException {
        Field schemaCache1 = AbstractSchemaManager.class.getDeclaredField("schemaCache");
        schemaCache1.setAccessible(true);
        Map<String, Map<String, String>> schemaCache = ((Map<String, Map<String, String>>) schemaCache1.get(mySQLSchemaManager));
        return schemaCache;
    }

    @After
    public void tearDown() throws Exception {
        try {
            remoteDataSource.close(true);
            transactionCache.stop();
            mySQLSchemaManager.stop();
            fileManager.stop();
            transactionCache.dispose();
            mySQLSchemaManager.dispose();
            fileManager.dispose();
            fileManager.destroy();
        } catch (Exception e) {
        }
    }
}
