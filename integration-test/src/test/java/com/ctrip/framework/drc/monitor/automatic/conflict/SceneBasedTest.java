package com.ctrip.framework.drc.monitor.automatic.conflict;

import com.ctrip.framework.drc.monitor.automatic.AutoTestDataSourceContainer;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Dec 16, 2019
 */
public class SceneBasedTest extends HashMap<Integer, DataSource> implements AutoTestDataSourceContainer {

    CustomizedBidirectionalStarter starter =
            new CustomizedBidirectionalStarter();

    @Before
    public void setUp() throws Exception {
        starter.setUp();
        starter.doTest();
        //init DataSource
        init(3306, 3307);
        //At present, replicator.start() returns before it really starts completely.
        // Here sleep 2s to wait util replicator init it's gtid executed.
        //Thread.sleep(3000);
    }

    @After
    public void tearDown() throws Exception {
        //close DataSource
        close(3306, 3307);
        //At present, starter.tearDown() throws NPE,
        // so tests should be one by one manually.
        starter.tearDown();
    }

    private void prepareExistingRow() throws InterruptedException {
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("insert into drc1.insert1 (id,three,four,datachange_lasttime) values (2,3,4,'2019-12-16 16:00:00.000')");
            assert statement.getUpdateCount() == 1 : "insert() should update 1 row, but update " + statement.getUpdateCount() + " rows."
                    + (statement.getUpdateCount() == 0 ? "\n probably the gtid is already used and the statement is auto skipped." : "");
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }

        try(Connection connection = get(3307).getConnection();
            Statement statement = connection.createStatement()
        ) {
            int reTry = 60;
            while(reTry > 0) {
                assert statement.execute("select * from drc1.insert1");
                try (ResultSet result = statement.getResultSet()) {
                    if (result.last()) {
                        break;
                    }
                }
                Thread.sleep(1000);
                reTry--;
            }
            System.out.println("retry: " + reTry + " times left");
            assert reTry > 0 : "prepared data is not transmitted to the opposite mysql master after 60s, " +
                    "\nand probably will never be.";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }

    @Test
    public void insertConcurrently() throws InterruptedException {
        try(Connection c3306 = get(3306).getConnection();
            Connection c3307 = get(3307).getConnection();
        ) {
            try (Statement s3306 = c3306.createStatement()) {
                assert !s3306.execute("insert into drc1.insert1 (id,three,four,datachange_lasttime) values (2,3,4,'2019-12-16 16:00:02.345')");
                assertEquals(1, s3306.getUpdateCount());
            }
            try (Statement s3307 = c3307.createStatement()) {
                assert !s3307.execute("insert into drc1.insert1 (id,three,four,datachange_lasttime) values (2,3,4,'2019-12-16 16:00:01.234')");
                assertEquals(1, s3307.getUpdateCount());
            }
            int reTry = 60;
            while(reTry > 0) {
                try {
                    verify(c3306, 2, "2019-12-16 16:00:02.345");
                    verify(c3307, 2, "2019-12-16 16:00:02.345");
                    break;
                } catch (Throwable t) {
                    reTry--;
                    Thread.sleep(1000);
                }
            }
            System.out.println("retry: " + reTry + " times left");
            assert reTry > 0 : "insertConcurrently() does not pass after 60s, and probably will never do.";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here: " + e.getMessage();
        }
    }


    @Test
    public void updateConcurrently() throws InterruptedException {
        prepareExistingRow();
        try(Connection c3306 = get(3306).getConnection();
            Connection c3307 = get(3307).getConnection();
        ) {
            try (Statement s3306 = c3306.createStatement()) {
                assert !s3306.execute("update drc1.insert1 set id=2,three=30,four=40,datachange_lasttime='2019-12-16 16:00:01.234' where id=2 AND datachange_lasttime='2019-12-16 16:00:00.000'");
            }
            try (Statement s3307 = c3307.createStatement()) {
                assert !s3307.execute("update drc1.insert1 set id=2,three=30,four=40,datachange_lasttime='2019-12-16 16:00:02.345' where id=2 AND datachange_lasttime='2019-12-16 16:00:00.000'");
            }
            int reTry = 60;
            while(reTry > 0) {
                try {
                    verify(c3306, 2, "2019-12-16 16:00:02.345");
                    verify(c3307, 2, "2019-12-16 16:00:02.345");
                    break;
                } catch (Throwable t) {
                    reTry--;
                    Thread.sleep(1000);
                }
            }
            System.out.println("retry: " + reTry + " times left");
            assert reTry > 0 : "updateConcurrently() does not pass after 60s, and probably will never do.";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }
    }

    private void truncate(Connection conn) {
        try (PreparedStatement stat = conn.prepareStatement("truncate table drc1.insert1")) {
            assert !stat.execute();
            assert stat.getUpdateCount() == 0;
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }
    }

    private void insert(Connection conn, int id, String lt) throws InterruptedException {
        String SQL = "insert into drc1.insert1 (id,datachange_lasttime) values (" + id + ",'" + lt +"')";
        boolean retry = true;
        while(retry) {
            retry = false;
            try (PreparedStatement stat = conn.prepareStatement(SQL)) {
                assert !stat.execute();
                assert stat.getUpdateCount() == 1 : "insert() should affect 1 rows but affect " + stat.getUpdateCount() + " rows"
                        + "\n" + SQL;
            } catch (MySQLTransactionRollbackException e) {
                //deadlock
                retry = true;
                Thread.sleep(1000);
            } catch (MySQLIntegrityConstraintViolationException e) {
                //conflict
                System.out.println("CONFLICT: " + SQL);
            } catch (SQLException e) {
                assert 1 == 2 : "program should never go here:" + e.getMessage()
                        + "\n" + SQL;
            }
        }
    }

    private void update(Connection conn, int newId, String newLt, int id, String lt) throws InterruptedException {
        String SQL = "update drc1.insert1 set id=" + newId +
                ",three=30,four=40,datachange_lasttime='" + newLt +
                "' where id=" + id + " AND datachange_lasttime='" + lt + "'";
        boolean retry = true;
        while(retry) {
            retry = false;
            try (PreparedStatement stat = conn.prepareStatement(SQL)) {
                assert !stat.execute();
                if (stat.getUpdateCount() > 0) {
                  assert stat.getUpdateCount() == 1 : "update() should affect less than 2 rows but affect " + stat.getUpdateCount() + " rows"
                          + "\n" + SQL;
                } else {
                    //conflict
                    System.out.println("CONFLICT: " + SQL);
                }
            } catch (MySQLTransactionRollbackException e) {
                //deadlock
                retry = true;
                Thread.sleep(1000);
            } catch (SQLException e) {
                e.printStackTrace();
                assert 1 == 2 : "program should never go here: " + e.getMessage()
                        + "\n" + SQL;
            }
        }
    }

    private void verify(Connection conn, int id, String lt) throws Throwable {
        try (PreparedStatement stat = conn.prepareStatement("select * from drc1.insert1 where id = ?")) {
            stat.setInt(1, id);
            assert stat.execute();
            try (ResultSet result = stat.getResultSet()) {
                assert result.last();
                assert lt.equals(result.getString(6));
            }
        } catch (Throwable t) {
            throw t;
        }
    }

    private void verify(Connection conn, int id, int index, String value) throws Throwable {
        try (PreparedStatement stat = conn.prepareStatement("select * from drc1.insert1 where id = ?")) {
            stat.setInt(1, id);
            assert stat.execute();
            try (ResultSet result = stat.getResultSet()) {
                assert result.last();
                assert value.equals(result.getString(index));
            }
        } catch (Throwable t) {
            throw t;
        }
    }

    @Test
    public void executeManyConcurrently() throws InterruptedException {
        ExecutorService server;
        try (Connection c3306 = get(3306).getConnection();
            Connection c3307 = get(3307).getConnection()) {
            server = new ThreadPoolExecutor(
                    2, 2, 0, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(2),
                    new ThreadPoolExecutor.AbortPolicy());
            truncate(c3306);
            truncate(c3307);
            server.execute(() -> {
                try {
                    insert(c3306, 1,"2019-12-16 16:00:00.000");
                    insert(c3306, 2,"2019-12-16 16:00:00.000");
                    insert(c3306, 3,"2019-12-16 16:00:00.000");
                    insert(c3306, 4,"2019-12-16 16:00:00.000");
                    insert(c3306, 5,"2019-12-16 16:00:00.000");
                    insert(c3306, 6,"2019-12-16 16:00:00.000");
                    insert(c3306, 7,"2019-12-16 16:00:00.000");
                    update(c3306, 1, "2019-12-16 16:00:00.002",
                            1, "2019-12-16 16:00:00.000");
                    update(c3306, 1, "2019-12-16 16:00:00.003",
                        1, "2019-12-16 16:00:00.002");
                    update(c3306, 1, "2019-12-16 16:00:00.004",
                            1, "2019-12-16 16:00:00.003");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            server.execute(() -> {
                try {
                    insert(c3307, 1,"2019-12-16 16:00:01.000");
                    insert(c3307, 2,"2019-12-16 16:00:01.000");
                    insert(c3307, 3,"2019-12-16 16:00:01.000");
                    insert(c3307, 4,"2019-12-16 16:00:01.000");
                    insert(c3307, 5,"2019-12-16 16:00:01.000");
                    insert(c3307, 6,"2019-12-16 16:00:01.000");
                    insert(c3307, 7,"2019-12-16 16:00:01.000");
                    update(c3307, 1, "2019-12-16 16:00:02.000",
                            1, "2019-12-16 16:00:01.000");
                    update(c3307, 1, "2019-12-16 16:00:03.000",
                            1, "2019-12-16 16:00:02.000");
                    update(c3307, 1, "2019-12-16 16:00:04.000",
                            1, "2019-12-16 16:00:03.000");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            server.shutdown();
            if (!server.awaitTermination(3, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
            int reTry = 60;
            while(reTry > 0) {
                try {
                    verify(c3306, 1, "2019-12-16 16:00:04.0");
                    verify(c3306, 2, "2019-12-16 16:00:01.0");
                    verify(c3306, 3, "2019-12-16 16:00:01.0");

                    verify(c3307, 1, "2019-12-16 16:00:04.0");
                    verify(c3307, 2, "2019-12-16 16:00:01.0");
                    verify(c3307, 3, "2019-12-16 16:00:01.0");

                    verify(c3307, 6, "2019-12-16 16:00:01.0");
                    break;
                } catch (Throwable t) {
                    reTry--;
                    Thread.sleep(1000);
                }
            }
            System.out.println("retry: " + reTry + " times left");
            assert reTry > 0 : "updateConcurrently() does not pass after 60s, and probably will never do.";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }
        //logEventHandler cannot be disposed in time,
        // look into AbstractMySQLSlave for a reason.
        //
        //It seems that it is fixed.
        //Thread.sleep(1000);
    }

    @Test
    public void updateOneByOne() throws InterruptedException {
        prepareExistingRow();
        try (Connection c3306 = get(3306).getConnection();
             Connection c3307 = get(3307).getConnection()) {
            for (int i = 0; i < 1000; i++) {
                try (Statement s3306 = c3306.createStatement()) {
                    assert !s3306.execute("update drc1.insert1 set three = " + i + " where id = 2");
                    assertEquals(1, s3306.getUpdateCount());
                }
            }
            int reTry = 60;
            while(reTry > 0) {
                try {
                    verify(c3307, 2, 4, "999");
                    break;
                } catch (Throwable t) {
                    reTry--;
                    Thread.sleep(1000);
                }
            }
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }
    }

    @Test
    public void batchInsert() throws InterruptedException {
        int batchCount = 2000;
        int count = 5;
        try (Connection c3306 = get(3306).getConnection();
             Connection c3307 = get(3307).getConnection()) {
            for (int i = 0; i < count; i++) {
                try (Statement s3306 = c3306.createStatement()) {
                    s3306.execute("begin;");
                    for (int j = 0; j < batchCount; j++) {
                        assert !s3306.execute("insert into drc1.insert1 (three) values (3)");
                        assertEquals(1, s3306.getUpdateCount());
                    }
                    s3306.execute("commit;");
                }
            }

            int reTry = 60;
            while(reTry > 0) {
                try {
                    for (int i = 1; i <= batchCount * count; i++) {
                        verify(c3307, i * 2, 4, "3");
                    }
                    break;
                } catch (Throwable t) {
                    reTry--;
                    Thread.sleep(1000);
                }
            }
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here:" + e.getMessage();
        }
    }
}
