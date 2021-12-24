package com.ctrip.framework.drc.applier.confirmed.mysql;

import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.Test;

/**
 * @Author Slight
 * Oct 18, 2019
 */
public class ManualTest {

    @Test
    public void closeWithoutCommit() throws Exception {
        //We must make sure that
        // we don't need to call connection.setAutoCommit(false)
        PoolProperties properties = new PoolProperties();
        properties.setUrl("jdbc:mysql://127.0.0.1:3306?useSSL=false&useUnicode=true&characterEncoding=UTF-8");
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("root");
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=2000");
        //LinkContextResource modified.
        // schemas history fails to init, which is not import to this case.
        // link context here is only used to init DataSource.
        // try to catch the exception.

        //We could find that
        // as long as resource.begin() is called,
        //      |
        //      +- statement.execute("begin");
        //
        // data wouldn't be applied unless commit() is called.
        //Without resource.begin() called,
        // as auto commit is set to true by default,
        // every single RowsEvent is applied immediately.
    }

    @Test
    public void howToSetPoolSize() throws Exception {
        PoolProperties properties = new PoolProperties();
        properties.setUrl("jdbc:mysql://127.0.0.1:3306?useSSL=false&useUnicode=true&characterEncoding=UTF-8");
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("root");
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=2000");
        //properties below are about connection pool
        properties.setMaxActive(2);
        properties.setInitialSize(2);
        properties.setMinIdle(2);
        properties.setMaxIdle(2);
        //LinkContextResource modified.
        // schemas history fails to init, which is not import to this case.
        // link context here is only used to init DataSource.
        // try to catch the exception.

        //We dispose r1,
        //              |
        //              +- connection.close();
        // and we guess r3.initialize() will not block,
        // which is found to be true.

        //((Resource.Dynamic)r1).dispose();

        //If neither of r1 & r2 is disposed,
        // It blocks here, and timeout after 30 seconds.
    }

    @Test
    public void whenConnectionIsRecycled() throws Exception {
        PoolProperties properties = new PoolProperties();
        properties.setUrl("jdbc:mysql://127.0.0.1:3306?useSSL=false&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true");
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("root");
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=2000");
        //properties below are about connection pool
        properties.setMaxActive(1);
        properties.setInitialSize(1);
        properties.setMinIdle(1);
        properties.setMaxIdle(1);
        properties.setTestOnBorrow(true);
        properties.setValidationInterval(1);
        properties.setValidationQuery("SELECT 1");
        //LinkContextResource modified.
        // schemas history fails to init, which is not import to this case.
        // link context here is only used to init DataSource.
        // try to catch the exception.

        //resource.commit();
        //resource.rollback();

        //Even if commit() or rollback() is not completed successfully,
        // as long as markDiscard() is called,
        //                |
        //                +- connection.unwrap(PooledConnection.class).setDiscarded(true);
        // new connection borrowed from data source is cleaned up.

        //On the other hand,
        // if markDiscard() is not called, the connection remains to be 'dirty'
        // when it is re-borrowed from the connection pool.
    }

}
