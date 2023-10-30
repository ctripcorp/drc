package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.DbDisposeTask.Result;
import com.wix.mysql.distribution.Version;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.util.MySQLConstants.isUsed;

public class DbDisposeTaskTest {

    public String registryKey = "test";
    public int port = 9999;


    @Before
    public void cleanUp() {
        new RetryTask<>(new DbDisposeTask(port, registryKey, Version.v5_7_23)).call();
        new RetryTask<>(new DbDisposeTask(port, registryKey, Version.v8_0_32)).call();
    }

    @Test
    public void testGetVersionForMySQL8() throws IOException, InterruptedException {
        Assert.assertNull(DbDisposeTask.getExistingMysqlVersion(port));
        testGetVersion(Version.v5_7_23, port);
        testGetVersion(Version.v8_0_32, port);
    }

    @Test
    public void testDestroyRestoredMySQL() throws Exception {
        Version targetVersion = Version.v8_0_32;

        MySQLInstance useless = isUsed(port)
                ? new RetryTask<>(new DbRestoreTask(port, registryKey, targetVersion)).call()
                : new RetryTask<>(new DbCreateTask(port, registryKey, targetVersion)).call();

        MySQLInstance embeddedDb = isUsed(port)
                ? new RetryTask<>(new DbRestoreTask(port, registryKey, targetVersion)).call()
                : new RetryTask<>(new DbCreateTask(port, registryKey, targetVersion)).call();
        if (!(embeddedDb instanceof MySQLInstanceExisting)) {
            throw new Exception("expect existing mysql");
        }
        Assert.assertEquals(targetVersion, DbDisposeTask.getExistingMysqlVersion(port));
        embeddedDb.destroy();
        Assert.assertNull(DbDisposeTask.getExistingMysqlVersion(port));
    }


    @Test
    public void testDispose() throws IOException, InterruptedException {
        Version targetVersion = Version.v8_0_32;
        Version otherVersion = Version.v5_7_23;

        // 1. no process, no need
        Result result = new RetryTask<>(new DbDisposeTask(port, registryKey, targetVersion)).call();
        Assert.assertEquals(result, Result.NO_NEED);
        Assert.assertNull(DbDisposeTask.getExistingMysqlVersion(port));

        // start mysql
        MySQLInstance embeddedDb = isUsed(port)
                ? new RetryTask<>(new DbRestoreTask(port, registryKey, targetVersion)).call()
                : new RetryTask<>(new DbCreateTask(port, registryKey, targetVersion)).call();
        Assert.assertEquals(targetVersion, DbDisposeTask.getExistingMysqlVersion(port));

        // 2. target version, no need to dispose
        result = new RetryTask<>(new DbDisposeTask(port, registryKey, targetVersion)).call();
        Assert.assertEquals(result, Result.NO_NEED);
        Assert.assertEquals(targetVersion, DbDisposeTask.getExistingMysqlVersion(port));

        // 3. other version, shut it down
        result = new RetryTask<>(new DbDisposeTask(port, registryKey, otherVersion)).call();

        Assert.assertEquals(result, Result.SUCCESS);
        Assert.assertNull(DbDisposeTask.getExistingMysqlVersion(port));
    }

    public void testGetVersion(Version targetVersion, int port) throws IOException, InterruptedException {
        MySQLInstance embeddedMysql = new RetryTask<>(new DbCreateTask(port, "test", targetVersion)).call();
        Version existingMysqlVersion = DbDisposeTask.getExistingMysqlVersion(port);
        embeddedMysql.destroy();
        Assert.assertEquals(existingMysqlVersion, targetVersion);
    }
}
