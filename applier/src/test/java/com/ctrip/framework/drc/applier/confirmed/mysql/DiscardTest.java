package com.ctrip.framework.drc.applier.confirmed.mysql;

import org.apache.tomcat.jdbc.pool.PooledConnection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Jul 30, 2020
 */
public class DiscardTest extends ConflictTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void loopDiscarding() throws InterruptedException {
        ExecutorService service = new ThreadPoolExecutor(
                10, 10, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy()
        );
        Runnable r = () -> {
            while (true) {
                try (Connection connection = dataSource.getConnection();) {
                    logger.info("connection got - {}", new Date().toString());
                    connection.unwrap(PooledConnection.class).setDiscarded(true);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        for (int i = 0; i < 8; i++) {
            service.execute(r);
        }
        Thread.currentThread().join();
    }
}
