package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.wix.mysql.EmbeddedMysql;
import org.junit.After;
import org.junit.Test;


/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbInitTaskTest {

    private int PORT = 8989;

    private EmbeddedMysql embeddedMysql;

    @Test
    public void testDbInitTask() {
        embeddedMysql = new RetryTask<>(new DbInitTask(PORT, "ut_cluster")).call();
    }

    @After
    public void tearDown() {
        try {
            embeddedMysql.stop();
        } catch (Exception e) {
        }
    }
}
