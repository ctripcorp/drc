package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.wix.mysql.EmbeddedMysql;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;


/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbInitTaskTest {

    private int PORT = 8989;

    @Test
    public void testDbInitTask() {
        EmbeddedMysql embeddedMysql = new RetryTask<>(new DbInitTask(PORT)).call();
        Assert.assertTrue(isUsed( PORT));
        embeddedMysql.stop();
    }

    private static boolean isUsed(int port) {
        try (ServerSocket ignored = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
