package com.ctrip.framework.drc.monitor.automatic.conflict;

import com.ctrip.framework.drc.monitor.automatic.AutoTestDataSourceContainer;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * @Author Slight
 * Dec 16, 2019
 */
public class UnitConflictConfirmation extends HashMap<Integer, DataSource> implements AutoTestDataSourceContainer {

    CustomizedBidirectionalStarter starter =
            new CustomizedBidirectionalStarter();

    @Before
    public void setUp() throws Exception {
        starter.setUp();
        starter.doTest();
        //init DataSource
        init(3306);
    }

    @After
    public void tearDown() throws Exception {
        //close DataSource
        close(3306);
        //At present, starter.tearDown() throws NPE,
        // so tests should be one by one manually.
        starter.tearDown();
    }

    private void prepareExistingRow() {
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("insert into drc1.insert1 (id,three,four,datachange_lasttime) values (2,3,4,'2019-12-16 16:00:00.000')");
            assert statement.getUpdateCount() == 1 : "insert() should update 1 row, but update " + statement.getUpdateCount() + " rows."
                    + (statement.getUpdateCount() == 0 ? "\n probably the gtid is already used and the statement is auto skipped." : "");
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }


    @Test (expected = MySQLIntegrityConstraintViolationException.class)
    public void insertOnExist() throws Exception {
        prepareExistingRow();
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("insert into drc1.insert1 (id,three,four,datachange_lasttime) values (2,3,4,'2019-12-16 17:00:00.000')");
            assert 1 == 2 : "program should never go here.";
        } catch (MySQLIntegrityConstraintViolationException e) {
            throw e;
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }

    @Test
    public void updateOnExist() {
        prepareExistingRow();
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("update drc1.insert1 set three=30,four=40 where id = 2 AND datachange_lasttime='2019-12-16 17:00:00.000'");
            assert statement.getUpdateCount() == 0 : "update() should update 0 rows because of conflict, but update " + statement.getUpdateCount() + " rows";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }

    @Test
    public void updateOnEmpty() {
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("update drc1.insert1 set three=30,four=40 where id = 2 AND datachange_lasttime='2019-12-16 17:00:00.000'");
            assert statement.getUpdateCount() == 0 : "update() should update 0 rows because of conflict, but update " + statement.getUpdateCount() + " rows";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }

    @Test
    public void deleteOnEmpty() {
        try(Connection connection = get(3306).getConnection();
            Statement statement = connection.createStatement();
        ) {
            assert !statement.execute("delete from drc1.insert1 where id = 2 AND datachange_lasttime='2019-12-16 17:00:00.000'");
            assert statement.getUpdateCount() == 0 : "delete() should update 0 rows because of conflict, but update " + statement.getUpdateCount() + " rows";
        } catch (SQLException e) {
            assert 1 == 2 : "program should never go here.";
        }
    }
}
