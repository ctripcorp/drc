package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
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
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager.PORT_STEP;

/**
 * @Author limingdong
 * @create 2020/3/3
 */
public class MySQLSchemaManagerTest extends MockTest {

    @Mock
    private Filter<ITransactionEvent> filterChain;

    @Mock
    private ReplicatorConfig replicatorConfig;

    @Mock
    private UuidOperator uuidOperator;

    @Mock
    private UuidConfig uuidConfig;

    private Set<UUID> uuids = Sets.newHashSet();

    private static final String DB1_TABLE1 = "CREATE TABLE `drc100`.`insert100` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";


    private static final String DB1_TEMPLATE = "CREATE TABLE `drc100`.`%s` (\n" +
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

    private static final String ALTER_SQL = "ALTER TABLE `drc1`.`t` ADD COLUMN addcol129 VARCHAR(255) DEFAULT NULL COMMENT 'ab';";

    private static final String CLUSTER_NAME = "test-schema";

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
    public void testConcurrentSetup() {
        MySQLSchemaManager mySQLSchemaManager1 = new MySQLSchemaManager(endpoint, 5000, "test", null);
        MySQLSchemaManager mySQLSchemaManager2 = new MySQLSchemaManager(endpoint, 5001, "test", null);
        MySQLSchemaManager mySQLSchemaManager3 = new MySQLSchemaManager(endpoint, 5002, "test", null);
        MySQLSchemaManager mySQLSchemaManager4 = new MySQLSchemaManager(endpoint, 5003, "test", null);
        MySQLSchemaManager mySQLSchemaManager5 = new MySQLSchemaManager(endpoint, 5004, "test", null);
        MySQLSchemaManager mySQLSchemaManager6 = new MySQLSchemaManager(endpoint, 5005, "test", null);
        MySQLSchemaManager mySQLSchemaManager7 = new MySQLSchemaManager(endpoint, 5006, "test", null);
        MySQLSchemaManager mySQLSchemaManager8 = new MySQLSchemaManager(endpoint, 5007, "test", null);
        MySQLSchemaManager mySQLSchemaManager9 = new MySQLSchemaManager(endpoint, 5008, "test", null);
        MySQLSchemaManager mySQLSchemaManager10 = new MySQLSchemaManager(endpoint, 5009, "test", null);
        List<MySQLSchemaManager> mySQLSchemaManagers = Lists.newArrayList(mySQLSchemaManager1, mySQLSchemaManager2, mySQLSchemaManager3, mySQLSchemaManager4, mySQLSchemaManager5,
                mySQLSchemaManager6, mySQLSchemaManager7, mySQLSchemaManager8, mySQLSchemaManager9, mySQLSchemaManager10);

        int testCount = 10;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(testCount);
        ExecutorService executorService = Executors.newFixedThreadPool(testCount);
        for (int i = 0; i < testCount; i++) {
            executorService.execute(new Task(cyclicBarrier, mySQLSchemaManagers.get(i)));
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Assert.assertTrue(isUsed( 5000 + PORT_STEP));
        Assert.assertTrue(isUsed( 5001 + PORT_STEP));
        Assert.assertTrue(isUsed( 5002 + PORT_STEP));
        Assert.assertTrue(isUsed( 5003 + PORT_STEP));
        Assert.assertTrue(isUsed( 5004 + PORT_STEP));
        Assert.assertTrue(isUsed( 5005 + PORT_STEP));
        Assert.assertTrue(isUsed( 5006 + PORT_STEP));
        Assert.assertTrue(isUsed( 5007 + PORT_STEP));
        Assert.assertTrue(isUsed( 5008 + PORT_STEP));
        Assert.assertTrue(isUsed( 5009 + PORT_STEP));

        for (MySQLSchemaManager m : mySQLSchemaManagers) {
            try {
                m.start();
                m.stop();
                m.dispose();
            } catch (Exception e) {

            }
        }
    }

    @Test
    public void testFindSchema() throws Exception {
        testInstance(mySQLSchemaManager);
        mySQLSchemaManager.apply("drc1", ALTER_SQL);

        TableInfo remoteTableInfo = mySQLSchemaManager.queryTableInfoByIS(remoteDataSource, "drc1", "t");
        TableInfo localTableInfo = mySQLSchemaManager.find("drc1", "t");
        List<TableMapLogEvent.Column> columnList =  localTableInfo.getColumnList();
        for (TableMapLogEvent.Column column_is : columnList) {
            String columnName = column_is.getName();
            if ("addcol129".equalsIgnoreCase(columnName)) {
                Assert.assertNull(remoteTableInfo.getFieldMetaByName(columnName));
                continue;
            }
            TableMapLogEvent.Column column_remote = remoteTableInfo.getFieldMetaByName(columnName);
            Assert.assertEquals(column_is, column_remote);
        }

        recovery();
    }

    @Test
    public void testRecoveryException() throws Exception {
        DrcSchemaSnapshotLogEvent snapshotLogEvent = getDrcSchemaSnapshotLogEvent(true);
        boolean res = mySQLSchemaManager.recovery(snapshotLogEvent);
        Assert.assertTrue(res);

        snapshotLogEvent = getDrcSchemaSnapshotLogEvent(false);
        res = mySQLSchemaManager.recovery(snapshotLogEvent);
        Assert.assertFalse(res);
    }

    @Test
    public void testSystemVariables() throws Exception {
        int port = 5005;
        MySQLSchemaManager mySQLSchemaManager = new MySQLSchemaManager(endpoint, port, "test5005", null);
        mySQLSchemaManager.initialize();
        mySQLSchemaManager.start();
        DataSourceManager dataSourceManager = DataSourceManager.getInstance();

        Endpoint endpoint = new DefaultEndPoint(AllTests.SRC_IP, port + PORT_STEP, AllTests.MYSQL_USER, AllTests.MYSQL_PASSWORD);

        DataSource dataSource = dataSourceManager.getDataSource(endpoint);

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SHOW GLOBAL VARIABLES LIKE 'character_set_server'");
                resultSet.next();
                String css = resultSet.getString(2);
                Assert.assertEquals("utf8mb4", css);

                resultSet = statement.executeQuery("SHOW GLOBAL VARIABLES LIKE 'character_set_database'");
                resultSet.next();
                String csd = resultSet.getString(2);
                Assert.assertEquals("utf8mb4", csd);

                resultSet = statement.executeQuery("SHOW GLOBAL VARIABLES LIKE 'collation_server'");
                resultSet.next();
                String cs = resultSet.getString(2);
                Assert.assertEquals("utf8mb4_general_ci", cs);

                resultSet = statement.executeQuery("SHOW GLOBAL VARIABLES LIKE 'character_set_client'");
                resultSet.next();
                String csc = resultSet.getString(2);
                Assert.assertEquals("utf8mb4", csc);

                resultSet = statement.executeQuery("SHOW GLOBAL VARIABLES LIKE 'character_set_connection'");
                resultSet.next();
                csc = resultSet.getString(2);
                Assert.assertEquals("utf8mb4", csc);
            }
        } catch (SQLException e) {
        } finally {
            DataSourceManager.getInstance().clearDataSource(endpoint);
        }

        mySQLSchemaManager.stop();
        mySQLSchemaManager.dispose();
    }

    public static DrcSchemaSnapshotLogEvent getDrcSchemaSnapshotLogEvent(boolean good) {
        Map<String, String> table1 = Maps.newHashMap();
        Map<String, Map<String, String>> localDdls = Maps.newHashMap();
        if (good) {
            table1.put("insert100", DB1_TABLE1);
            for (int i = 0; i < 300; ++i) {
                String tableName = "batch" + i;
                table1.put(tableName, String.format(DB1_TEMPLATE, tableName));
            }
        } else {
            table1.put("insert100", "create");
        }
        localDdls.put("drc100", table1);

        return new DrcSchemaSnapshotLogEvent(localDdls, 0 , 10);
    }

    public void recovery() throws IOException {
        final ByteBuf byteBuf = initByteBuf();
        final GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        fileManager.append(Lists.newArrayList(gtidLogEvent.getLogEventHeader().getHeaderBuf(), gtidLogEvent.getPayloadBuf()), new TransactionContext(false));

        MySQLSchemaManager sm = new MySQLSchemaManager(endpoint, APPLIER_PORT + 1001, "test", null);
        FileManager fm = new DefaultFileManager(sm, CLUSTER_NAME);

        try {
            sm.initialize();
            fm.initialize();
            sm.start();
            fm.start();

            GtidSet gtidSet = fm.getExecutedGtids();
            Assert.assertTrue(gtidSet.toString() == "");

            Map<String, Map<String, String>> ss = sm.snapshot();
            Map<String, Map<String, String>> ssOrigin = mySQLSchemaManager.snapshot();

            Assert.assertFalse(ss.isEmpty());
            Assert.assertFalse(ssOrigin.isEmpty());
            Assert.assertTrue(ss.size() == ssOrigin.size());

            for (Map.Entry<String, Map<String, String>> entry : ssOrigin.entrySet()) {
                String key = entry.getKey();
                Map<String, String> ssValue = ss.get(key);
                Assert.assertNotNull(ssValue);

                for (Map.Entry<String, String> en : entry.getValue().entrySet()) {
                    String table = en.getKey();
                    String ssCreateTable = ssValue.get(table);
                    Assert.assertNotNull(ssCreateTable);
                    Assert.assertEquals(en.getValue(), ssCreateTable);
                }
            }
        } catch (Exception e){

        }
    }

    private void testInstance(MySQLSchemaManager mySQLSchemaManager) {
        mySQLSchemaManager.clone(endpoint);
        Set<TableId> tableIds = mySQLSchemaManager.getTableIds(remoteDataSource);
        for (TableId tableId : tableIds) {
            if (StringUtils.isNotBlank(tableId.getTableName())) {
                TableInfo remoteTableInfo = mySQLSchemaManager.queryTableInfoByIS(remoteDataSource, tableId.getDbName(), tableId.getTableName());
                TableInfo localTableInfo = mySQLSchemaManager.find(tableId.getDbName(), tableId.getTableName());

                List<TableMapLogEvent.Column> columnList =  remoteTableInfo.getColumnList();
                for (TableMapLogEvent.Column column_is : columnList) {
                    String columnName = column_is.getName();
                    TableMapLogEvent.Column column_memory = localTableInfo.getFieldMetaByName(columnName);
                    Assert.assertEquals(column_is, column_memory);
                }
            }
        }
    }

    private ByteBuf initByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(65);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    public class Task implements Runnable {

        private CyclicBarrier cyclicBarrier;

        private MySQLSchemaManager mySQLSchemaManager;

        public Task(CyclicBarrier cyclicBarrier, MySQLSchemaManager mySQLSchemaManager) {
            this.cyclicBarrier = cyclicBarrier;
            this.mySQLSchemaManager = mySQLSchemaManager;
        }

        @Override
        public void run() {
            try {
                cyclicBarrier.await();
                mySQLSchemaManager.initialize();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
