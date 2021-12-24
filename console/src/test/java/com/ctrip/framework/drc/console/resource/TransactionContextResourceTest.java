package com.ctrip.framework.drc.console.resource;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-22
 */
public class TransactionContextResourceTest {

    private TransactionContextResource resource;

    private DataSource dataSource;

    @Before
    public void setUp() {
        PoolProperties properties = new PoolProperties();
        String url = String.format(SystemConfig.JDBC_URL_FORMAT, "127.0.0.1", 13306);
        properties.setUrl(url);
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("1234");
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=5000");
        properties.setInitialSize(2);
        properties.setMaxActive(1000);

        dataSource = new DataSource(properties);
    }

    @Test
    public void test() throws Exception {
        resource = new TransactionContextResource(dataSource);
        resource.initialize();
        String executedGtid = resource.getExecutedGtid();
        System.out.println(executedGtid);
        resource.setGtid("1e698014-90e5-11e9-a232-56d52b562cdd:861");
        resource.begin();
        resource.commit();
    }

    @After
    public void tearDown() throws Exception {
        resource.dispose();
    }
}
