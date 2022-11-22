package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.isUsed;
import static com.ctrip.framework.drc.core.server.utils.FileUtil.deleteFiles;
import static ctrip.framework.drc.mysql.EmbeddedDb.mysqlInstanceDir;

/**
 * @Author limingdong
 * @create 2022/10/27
 */
public class DbRestoreTaskTest {

    private static final String name = RandomStringUtils.randomAlphabetic(10);

    private static final int port = RandomUtils.nextInt(8383, 8900);

    private DbRestoreTask dbRestoreTask;

    private MySQLInstance embeddedMysql;

    private MySQLInstance restoredMysql;

    @After
    public void tearDown() {
        deleteFiles(new File(mysqlInstanceDir(name, port)));
    }

    @Before
    public void setUp() throws Exception {
        embeddedMysql = new RetryTask<>(new DbCreateTask(port, name)).call();
    }

    @Test
    public void testDbRestoreTask() throws Exception {
        dbRestoreTask = new DbRestoreTask(port, name);
        restoredMysql = dbRestoreTask.call();
        Assert.assertNotNull(restoredMysql);
        Assert.assertTrue(isUsed(port));
        restoredMysql.destroy();
        Assert.assertFalse(isUsed(port));
    }
}