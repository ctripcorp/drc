package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.exception.DdlException;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Maps;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.manager.task.BatchTask.MAX_BATCH_SIZE;
import static com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask.MAX_RETRY;
import static com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager.MAX_ACTIVE;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTaskTest extends AbstractSchemaTest {

    private Map<String, Map<String, String>> ddlSchemas = Maps.newHashMap();

    private static final String DB_NAME_1 = "drc3";

    private static final String DB_NAME_2 = "drc4";

    private static final String TABLE_NAME = "utname";

    private static final String TABLE_CREATE = "CREATE TABLE `%s`.`%s`(`id` int(11) NOT NULL AUTO_INCREMENT,`one` varchar(30) DEFAULT 'one',`two` varchar(1000) DEFAULT 'two',`three` char(30),`four` char(255),`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8; ; ";

    private int DB1_SIZE = MAX_BATCH_SIZE * MAX_ACTIVE + 20;

    private int DB2_SIZE = MAX_BATCH_SIZE * 3 + 20;

    private static final int ORIGIN_MAX_BATCH_SIZE = MAX_BATCH_SIZE;

    private static final int LOOP_SIZE = 10;

    private static int ASSERT_LOOP_SIZE = 0;

    private static int FAIL_SIZE = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MAX_BATCH_SIZE = 2;
        DB1_SIZE = MAX_BATCH_SIZE * MAX_ACTIVE + 20;

        DB2_SIZE = MAX_BATCH_SIZE * 3 + 20;

        Map<String, String> table1 = Maps.newHashMap();
        for (int i = 1; i <= DB1_SIZE; ++i) {
            String tableName = TABLE_NAME + i;
            table1.put(tableName, String.format(TABLE_CREATE, DB_NAME_1, tableName));
        }
        ddlSchemas.put(DB_NAME_1, table1);

        Map<String, String> table2 = Maps.newHashMap();
        for (int i = 1; i <= DB2_SIZE; ++i) {
            String tableName = TABLE_NAME + i;
            table2.put(tableName, String.format(TABLE_CREATE, DB_NAME_2, tableName));
        }
        ddlSchemas.put(DB_NAME_2, table2);
    }

    @Override
    protected AbstractSchemaTask getAbstractSchemaTask() {
        return new SchemeCloneTask(ddlSchemas, inMemoryEndpoint, inMemoryDataSource);
    }

    @Test
    public void testOnBatch() {
        // test false
        List<BatchTask> tasks = Lists.newArrayList();
        for (int i = 0; i < LOOP_SIZE; ++i) {
            tasks.add(new MockBatchTask(inMemoryEndpoint, inMemoryDataSource, i));
        }
        Assert.assertFalse(abstractSchemaTask.oneBatch(tasks));

        // test exception
        tasks.clear();
        for (int i = LOOP_SIZE; i < LOOP_SIZE + LOOP_SIZE; ++i) {
            tasks.add(new MockBatchTask(inMemoryEndpoint, inMemoryDataSource, i));
        }
        Assert.assertFalse(abstractSchemaTask.oneBatch(tasks));

        // test true
        tasks.clear();
        for (int i = LOOP_SIZE + 2; i < LOOP_SIZE + LOOP_SIZE + 2; i= i + 2) {
            tasks.add(new MockBatchTask(inMemoryEndpoint, inMemoryDataSource, i));
        }
        Assert.assertTrue(abstractSchemaTask.oneBatch(tasks));
    }

    @Test
    public void testRetry() {
        List<String> tasks = Lists.newArrayList();
        for (int i = 0; i < LOOP_SIZE; ++i) {
            tasks.add(String.valueOf(i));
        }
        Assert.assertNull(new RetryTask<>(new MockSchemeCloneTask(tasks, inMemoryEndpoint, inMemoryDataSource)).call());
        Assert.assertEquals(ASSERT_LOOP_SIZE, MAX_RETRY + 1);
        Assert.assertEquals(FAIL_SIZE, 1);
    }

    @After
    public void tearDown() {
        MAX_BATCH_SIZE = ORIGIN_MAX_BATCH_SIZE;
    }

    @Test
    public void testExecuteBatch() throws Exception {
        Connection connection = null;
        String excluded_db_1 = "drc1";
        String excluded_db_2 = "drc2";
        MySQLConstants.EXCLUDED_DB.add(excluded_db_1);
        MySQLConstants.EXCLUDED_DB.add(excluded_db_2);
        try {
            connection = inMemoryDataSource.getConnection();

            List<String> databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertFalse(databases.contains(DB_NAME_1));
            Assert.assertFalse(databases.contains(DB_NAME_2));

            // create db
            boolean res = abstractSchemaTask.call();
            Assert.assertTrue(res);

            databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertTrue(databases.contains(DB_NAME_1));
            Assert.assertTrue(databases.contains(DB_NAME_2));

            assertResultSize(connection, DB_NAME_1, DB1_SIZE);
            assertResultSize(connection, DB_NAME_2, DB2_SIZE);

            // clear schema
            SchemeClearTask task = new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource);
            res = task.call();
            Assert.assertTrue(res);

            databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertFalse(databases.contains(DB_NAME_1));
            Assert.assertFalse(databases.contains(DB_NAME_2));
        } finally {
            if (connection != null) {
                connection.close();
            }
            MySQLConstants.EXCLUDED_DB.remove(excluded_db_1);
            MySQLConstants.EXCLUDED_DB.remove(excluded_db_2);
        }
    }

    static class MockBatchTask extends BatchTask {

        private int loop;

        public MockBatchTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource, int loop) {
            super(inMemoryEndpoint, inMemoryDataSource);
            this.loop = loop;
        }

        @Override
        public Boolean call() {
            if (loop == LOOP_SIZE) {
                throw new RuntimeException();
            }
            if (loop % 2 == 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    static class MockBatchTask2 extends BatchTask {

        private List<String> sqls;

        public MockBatchTask2(List<String> sqls, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
            super(inMemoryEndpoint, inMemoryDataSource);
            this.sqls = sqls;
        }

        @Override
        public Boolean call() {
            throw new RuntimeException();
        }

        @Override
        protected List<BatchTask> getBatchTasks(Collection<String> sqlCollection, Class<? extends BatchTask> clazz) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
            List<BatchTask> tasks = com.google.common.collect.Lists.newArrayList();

            List<String> sqls = com.google.common.collect.Lists.newArrayList();
            int sqlSize = 0;
            for (String sql : sqlCollection) {
                sqls.add(sql);
                sqlSize++;

                if (sqlSize >= MAX_BATCH_SIZE) {
                    tasks.add(getBatchTask(clazz, sqls));
                    sqls = com.google.common.collect.Lists.newArrayList();
                    sqlSize = 0;
                }
            }

            if (sqlSize > 0) {
                tasks.add(getBatchTask(clazz, sqls));
            }

            return tasks;
        }
    }

    class MockSchemeCloneTask extends BatchTask {

        private List<String> sqls;

        public MockSchemeCloneTask(List<String> sqls, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
            super(inMemoryEndpoint, inMemoryDataSource);
            this.sqls = sqls;
        }

        @Override
        public void afterException(Throwable t) {
            super.afterException(t);
            ASSERT_LOOP_SIZE++;
        }

        @Override
        public void  afterFail() {
            FAIL_SIZE++;
        }

        @Override
        public Boolean call() throws Exception {
            boolean res = doCreate(sqls, MockBatchTask2.class, false);
            if (!res) {
                throw new DdlException(null);
            }
            return res;
        }
    }
}
