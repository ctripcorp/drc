package com.ctrip.framework.drc.applier.resource.mysql;

import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.ctrip.framework.drc.core.driver.pool.DrcConnectionPool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author limingdong
 * @create 2020/10/16
 */
public class DataSourceResourceTest {

    private static final String ROOT = "root";

    private static final String PASSWORD = "123456";

    private CountDownLatch latch = new CountDownLatch(100);

    private ExecutorService executorService = Executors.newFixedThreadPool(100);

    private final Logger logger = LoggerFactory.getLogger(DataSourceResourceTest.class);

    @Test
    public void testGetConnection() throws Exception {
        DataSourceResource dataSourceResource = new DataSourceResource();
        dataSourceResource.registryKey = "test.registryKey";
        ExecutorResource executorResource = new ExecutorResource();
        executorResource.initialize();
        executorResource.start();
        dataSourceResource.executor = executorResource;
        dataSourceResource.password = ROOT;
        dataSourceResource.username = ROOT;
        dataSourceResource.poolSize = 0;
        dataSourceResource.URL = "jdbc:mysql://10.10.10.10:3306?allowMultiQueries=true&useLocalSessionState=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
        dataSourceResource.initialize();
        dataSourceResource.start();
        try {
            dataSourceResource.getConnection();
        } catch (Throwable t) {}
        executorResource.stop();
        executorResource.dispose();
        dataSourceResource.stop();
        dataSourceResource.dispose();
    }

    @Test
    public void testWaitTimeout() throws Exception {
        List<Connection> connectionList = new ArrayList<>();
        DrcConnectionPool.SESSION_WAIT_TIMEOUT = 1;
        DataSourceResource dataSourceResource = new DataSourceResource();
        dataSourceResource.registryKey = "test.registryKey";
        ExecutorResource executorResource = new ExecutorResource();
        executorResource.initialize();
        executorResource.start();
        dataSourceResource.executor = executorResource;
        dataSourceResource.password = PASSWORD;
        dataSourceResource.username = ROOT;
        dataSourceResource.poolSize = 100;
        dataSourceResource.validationInterval = 500;
        dataSourceResource.URL = "jdbc:mysql://127.0.0.1:3306?allowMultiQueries=true&useLocalSessionState=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8";
        dataSourceResource.initialize();
        dataSourceResource.start();

        for (int i = 0; i < 100; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        conn = dataSourceResource.getConnection();
                        conn.setAutoCommit(true);
                        Statement statement = conn.createStatement();
                        ResultSet rs = statement.executeQuery("show session variables like 'wait_timeout'");
                        rs.next();
                        Assert.assertEquals(DrcConnectionPool.SESSION_WAIT_TIMEOUT, rs.getInt(2));
                        statement.close();
                        connectionList.add(conn);
                    } catch (Exception e) {
                        logger.error("dataSource test exception" + e);
                        Assert.fail();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await(4, TimeUnit.SECONDS);
        int returnSize = connectionList.size();
        closeConnection(connectionList);
        connectionList.clear();

        Thread.sleep(1010);  // let wait_time affect, all connections were closed
        for(int i = 0; i < returnSize; ++i) {
            try {
                Connection connection = dataSourceResource.getConnection();  //validate when borrow, create new connection
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("show session variables like 'wait_timeout'");
                rs.next();
                Assert.assertEquals(DrcConnectionPool.SESSION_WAIT_TIMEOUT, rs.getInt(2));
                connectionList.add(connection);
            } catch (Exception e){
                logger.error("dataSource test exception" + e);
                Assert.fail();
            }
        }

        closeConnection(connectionList);

        executorResource.stop();
        executorResource.dispose();
        dataSourceResource.stop();
        dataSourceResource.dispose();
    }

    private void closeConnection(List<Connection> connectionList) throws SQLException {
        for (int i = 0; i < connectionList.size(); i++) {
            Connection conn = connectionList.get(i);
            conn.close();
        }
    }
}
