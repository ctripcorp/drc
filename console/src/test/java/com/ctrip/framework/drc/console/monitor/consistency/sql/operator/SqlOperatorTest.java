package com.ctrip.framework.drc.console.monitor.consistency.sql.operator;

import ch.vorburger.exec.ManagedProcessException;
import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.operator.DefaultReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;

import static com.ctrip.framework.drc.console.AllTests.ciEndpoint;
import static com.ctrip.framework.drc.console.AllTests.ciWriteSqlOperatorWrapper;
import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.DEFAULT_FETCH_SIZE;
import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.QUERY_TIMEOUT_INSECOND;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.CONNECTION_TIMEOUT;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.JDBC_URL_FORMAT;

public class SqlOperatorTest {

    public Logger logger = LoggerFactory.getLogger(getClass());

    public static final String SELECT_SQL = "select * from `testdb`.`customer` where `datachange_lasttime` < '2021-04-01';";
    public static final String SELECT_1 = "select 1;";

    private Endpoint endpoint;

    private LocalSqlOperator localSqlOperator;
    private StreamQuerySqlOperator streamQuerySqlOperator;

    private Field connectionField;
    private Field statementField;

    @Before
    public void setUp() throws NoSuchFieldException {
        endpoint = new DefaultEndPoint(CI_MYSQL_IP, CI_PORT1, CI_MYSQL_USER, CI_MYSQL_PASSWORD);
        Class<?> clazz = DefaultReadResource.class;
        connectionField = clazz.getDeclaredField("connection");
        connectionField.setAccessible(true);
        statementField = clazz.getDeclaredField("statement");
        statementField.setAccessible(true);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void queryDbs() throws ManagedProcessException {
        List<String> dbs = MySqlUtils.queryDbsWithFilter(ciEndpoint, "");
        System.out.println(dbs);
    }

    @Test
    public void testClosableWhileSocketTimeout() throws Exception {

        AllTests.dropAllCiDb();
        AllTests.insertCiMuchDataDb(100000);

        // test1: no socket timeout
        PoolProperties poolProperties = getPoolProperties(endpoint);

        localSqlOperator = new LocalSqlOperator(endpoint, poolProperties);
        localSqlOperator.initialize();
        localSqlOperator.start();

        ReadResource readResource = null;
        try {
            readResource = localSqlOperator.select(new GeneralSingleExecution(SELECT_SQL));
        } catch (Exception e) {
            logger.error("fail select", e);
        }
        Assert.assertNotNull(readResource);
        Object connectionObject = connectionField.get(readResource);
        Object statementObject = statementField.get(readResource);
        Assert.assertNotNull(connectionObject);
        Assert.assertNotNull(statementObject);
        Assert.assertNotNull(readResource.getResultSet());
        readResource.close();

        localSqlOperator.stop();
        localSqlOperator.dispose();

        // test2: socket timeout
        poolProperties = getPoolProperties(endpoint);
        String timeout = String.format("connectTimeout=%s;socketTimeout=5", CONNECTION_TIMEOUT);
        poolProperties.setConnectionProperties(timeout);

        localSqlOperator = new LocalSqlOperator(endpoint, poolProperties);
        localSqlOperator.initialize();
        localSqlOperator.start();

        readResource = null;
        try {
            readResource = localSqlOperator.select(new GeneralSingleExecution(SELECT_SQL));
        } catch (Exception e) {
            logger.error("fail select", e);
        }
        Assert.assertNotNull(readResource);
        connectionObject = connectionField.get(readResource);
        statementObject = statementField.get(readResource);
        Assert.assertNotNull(connectionObject);
        Assert.assertNotNull(statementObject);
        Assert.assertNull(readResource.getResultSet());
        readResource.close();

        localSqlOperator.stop();
        localSqlOperator.dispose();
    }

    @Test
    public void testClosableWhileSocketTimeoutForStream() throws Exception {

        AllTests.dropAllCiDb();
        AllTests.insertCiMuchDataDb(100000);

        // test1: no socket timeout
        PoolProperties poolProperties = getPoolProperties(endpoint);

        streamQuerySqlOperator = new StreamQuerySqlOperator(endpoint, poolProperties, DEFAULT_FETCH_SIZE, QUERY_TIMEOUT_INSECOND);
        streamQuerySqlOperator.initialize();
        streamQuerySqlOperator.start();

        ReadResource readResource = null;
        try {
            readResource = streamQuerySqlOperator.select(new GeneralSingleExecution(SELECT_SQL));
        } catch (Exception e) {
            logger.error("fail select", e);
        }
        Assert.assertNotNull(readResource);
        Object connectionObject = connectionField.get(readResource);
        Object statementObject = statementField.get(readResource);
        Assert.assertNotNull(connectionObject);
        Assert.assertNotNull(statementObject);
        Assert.assertNotNull(readResource.getResultSet());
        readResource.close();

        streamQuerySqlOperator.stop();
        streamQuerySqlOperator.dispose();

        // test2: socket timeout
        poolProperties = getPoolProperties(endpoint);
        String timeout = String.format("connectTimeout=%s;socketTimeout=5", CONNECTION_TIMEOUT);
        poolProperties.setConnectionProperties(timeout);

        streamQuerySqlOperator = new StreamQuerySqlOperator(endpoint, poolProperties, DEFAULT_FETCH_SIZE, QUERY_TIMEOUT_INSECOND);
        streamQuerySqlOperator.initialize();
        streamQuerySqlOperator.start();

        readResource = null;
        try {
            readResource = streamQuerySqlOperator.select(new GeneralSingleExecution(SELECT_SQL));
        } catch (Exception e) {
            logger.error("fail select", e);
        }
        Assert.assertNotNull(readResource);
        connectionObject = connectionField.get(readResource);
        statementObject = statementField.get(readResource);
        Assert.assertNotNull(connectionObject);
        Assert.assertNotNull(statementObject);
        Assert.assertNull(readResource.getResultSet());
        readResource.close();

        streamQuerySqlOperator.stop();
        streamQuerySqlOperator.dispose();
    }

//    @Test
    public void testTime() {
        AllTests.dropAllCiDb();
        AllTests.insertCiMuchDataDb(0);
        long start = System.currentTimeMillis();
        GeneralSingleExecution execution = new GeneralSingleExecution(SELECT_1);
        ReadResource readResource = null;
        try {
            readResource = ciWriteSqlOperatorWrapper.select(execution);
        } catch (SQLException throwables) {
            logger.error("Failed execute : {}", SELECT_1, throwables);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        long end = System.currentTimeMillis();
        logger.info("took {}ms while no data", end - start);

        int count = 100000;
        AllTests.dropAllCiDb();
        AllTests.insertCiMuchDataDb(count);
        start = System.currentTimeMillis();
        execution = new GeneralSingleExecution(SELECT_SQL);
        readResource = null;
        try {
            readResource = ciWriteSqlOperatorWrapper.select(execution);
        } catch (SQLException throwables) {
            logger.error("Failed execute : {}", SELECT_SQL, throwables);
        } finally {
            if(readResource != null) {
                readResource.close();
            }
        }
        end = System.currentTimeMillis();
        logger.info("took {}ms for selecting {} rows", end - start, count);
    }

    private PoolProperties getPoolProperties(Endpoint endpoint) {
        PoolProperties poolProperties = new PoolProperties();
        String jdbcUrl = String.format(JDBC_URL_FORMAT, endpoint.getHost(), endpoint.getPort());
        poolProperties.setUrl(jdbcUrl);

        poolProperties.setUsername(endpoint.getUser());
        poolProperties.setPassword(endpoint.getPassword());
        poolProperties.setMaxActive(2);
        poolProperties.setMinIdle(1);
        poolProperties.setInitialSize(1);
        poolProperties.setMaxWait(10000);
        poolProperties.setMaxAge(28000000);
        String timeout = String.format("connectTimeout=%s;socketTimeout=10000", CONNECTION_TIMEOUT);
        poolProperties.setConnectionProperties(timeout);

        poolProperties.setValidationInterval(30000);
        return poolProperties;
    }

    public static final class LocalSqlOperator extends AbstractSqlOperator {

        public LocalSqlOperator(Endpoint endpoint, PoolProperties poolProperties) {
            super(endpoint, poolProperties);
        }
    }
}
