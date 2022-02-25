package com.ctrip.framework.drc.applier.resource;

import com.ctrip.framework.drc.applier.confirmed.mysql.ConflictTest;
import com.ctrip.framework.drc.applier.resource.position.TransactionTableResource;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Created by jixinwang on 2021/9/18
 */
public class TransactionTableResourceTest extends ConflictTest {

    private static final String COMMIT = "commit";

    private CountDownLatch latch;

    private static TransactionTableResource transactionTable;

    private ExecutorService executor = ThreadUtils.newFixedThreadPool(3, "TransactionTableTest");

    @Before
    public void before() throws Exception {
        System.setProperty(SystemConfig.TRANSACTION_TABLE_SIZE, "50");
        System.setProperty(SystemConfig.TRANSACTION_TABLE_MERGE_SIZE, "10");
        transactionTable = new TransactionTableResource();
        transactionTable.ip = "127.0.0.1";
        transactionTable.port = 3306;
        transactionTable.username = "root";
        transactionTable.password = "123456";
        transactionTable.initialize();
        initTransactionTable();
        latch = new CountDownLatch(3);
        delete();
    }

    private void initTransactionTable() throws SQLException {
        String createDbSql = "CREATE DATABASE IF NOT EXISTS drcmonitordb;";
        String createTransactionTable = "CREATE TABLE IF NOT EXISTS drcmonitordb.gtid_executed (\n" +
                "    id int(11) NOT NULL,\n" +
                "    server_uuid char(36) NOT NULL,\n" +
                "    gno bigint(20) NOT NULL,\n" +
                "    gtidset longtext,\n" +
                "  primary key ix_gtid (id, server_uuid)\n" +
                ");";
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(createDbSql)) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(createTransactionTable)) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                statement.execute();
            }
        }
    }

    @Test
    public void testBeginAndRollback() throws Exception {
        //test begin
        transactionTable.begin("uuid1:1");
        Assert.assertTrue(transactionTable.getBeginState().get(1));

        //test rollback
        transactionTable.rollback("uuid1:1");
        Assert.assertFalse(transactionTable.getBeginState().get(1));
    }

    @Test
    public void testRecord() {
        try (Connection connection = dataSource.getConnection()) {
            transactionTable.record(connection, "uuid1:1");
            try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                statement.execute();
            }
            String sql = "select gno from drcmonitordb.gtid_executed where id = 1";
            long gno = (long)select(sql);
            Assert.assertEquals(1, gno);
        } catch (SQLException e) {
        }
    }

    @Test
    public void testMerge() {
        for (int i = 0; i < 20; i++) {
            try (Connection connection = dataSource.getConnection()) {
                String gtid = "uuid1:" + i;
                transactionTable.begin(gtid);
                transactionTable.record(connection, gtid);
                try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                    statement.execute();
                }
                transactionTable.commit(gtid);
            } catch (SQLException | InterruptedException e) {
            }
        }

        String selectGtidSetSql = "select gtidset from drcmonitordb.gtid_executed where id = -1;";
        String gtidSet = (String) select(selectGtidSetSql);
        Assert.assertEquals("uuid1:0-19", gtidSet);
    }


    private Object select(String sql) {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    if (resultSet.next()) {
                        return resultSet.getObject(1);
                    }
                }
            }
        } catch (SQLException e) {
        }
        return StringUtils.EMPTY;
    }

    private void delete() throws SQLException {
        String deleteSql = "delete from drcmonitordb.gtid_executed;";
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(deleteSql)) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement(COMMIT)) {
                statement.execute();
            }
        }
    }

    @Test
    public void testWaitBegin() throws InterruptedException {
        String gtid1 = "uuid1:" + 1;
        String gtid2 = "uuid1:" + 51;
        String gtid3 = "uuid1:" + 101;

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    transactionTable.begin(gtid1);
                    Assert.assertEquals(true, transactionTable.getBeginState().get(1));
                    Thread.sleep(100);
                    transactionTable.commit(gtid1);
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    transactionTable.begin(gtid2);
                    Assert.assertEquals(true, transactionTable.getBeginState().get(1));
                    Thread.sleep(200);
                    transactionTable.commit(gtid2);
                    latch.countDown();
                } catch (InterruptedException e) {
                }
            }
        });

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    transactionTable.begin(gtid3);
                    Assert.assertEquals(true, transactionTable.getBeginState().get(1));
                    Thread.sleep(300);
                    transactionTable.commit(gtid3);
                    latch.countDown();
                } catch (InterruptedException e) {
                }
            }
        });
        latch.await();
    }
}
