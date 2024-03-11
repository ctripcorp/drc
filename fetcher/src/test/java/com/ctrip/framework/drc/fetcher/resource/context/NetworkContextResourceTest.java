package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.DbTransactionTableGtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.GtidReader;
import com.ctrip.framework.drc.core.driver.binlog.gtid.db.TransactionTableGtidReader;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/5/28
 */
public class NetworkContextResourceTest {

    public String initialGtidExecuted = "712c708a-2fa6-11eb-b7e5-98039ba92412:6-377";

    public String registryKey = "test.registryKey";

    private TestNetworkContextResource networkContextResource = new TestNetworkContextResource();

    @Before
    public void setUp() throws Exception {
        networkContextResource.initialGtidExecuted = initialGtidExecuted;
        networkContextResource.registryKey = registryKey;
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testEmptyGtidSet() throws Exception {
        networkContextResource.initialize();
        GtidSet gtidSet = networkContextResource.fetchGtidSet();
        Assert.assertEquals(gtidSet, new GtidSet("712c708a-2fa6-11eb-b7e5-98039ba92412:1-377"));
    }

    @Test
    public void testNotEmptyGtidSet() throws Exception {
        networkContextResource.initialGtidExecuted = "a33ded23-6960-11eb-a8e0-fa163e02998c:1-58";
        networkContextResource.initialize();
        GtidSet gtidSet = networkContextResource.fetchGtidSet();
        Assert.assertEquals(gtidSet, new GtidSet("712c708a-2fa6-11eb-b7e5-98039ba92412:1-6,a33ded23-6960-11eb-a8e0-fa163e02998c:1-58"));
    }

    @Test
    public void testEmptyPositionFromDb() throws Exception {
        networkContextResource.emptyPositionFromDb = true;
        networkContextResource.initialGtidExecuted = "a33ded23-6960-11eb-a8e0-fa163e02998c:1-58";
        networkContextResource.initialize();
        GtidSet gtidSet = networkContextResource.fetchGtidSet();
        Assert.assertEquals(new GtidSet(StringUtils.EMPTY), gtidSet);
    }


    @Test
    public void testQueryGtidFromDB() {
        NetworkContextResource networkContextResource1 = new NetworkContextResource();

        networkContextResource1.applyMode = ApplyMode.transaction_table.getType();
        List<GtidReader> executedGtidReaders = networkContextResource1.getExecutedGtidReaders(null);
        Assert.assertTrue(executedGtidReaders.stream().anyMatch(e -> e instanceof TransactionTableGtidReader));
        Assert.assertFalse(executedGtidReaders.stream().anyMatch(e -> e instanceof DbTransactionTableGtidReader));

        networkContextResource1.applyMode = ApplyMode.db_transaction_table.getType();
        networkContextResource1.includedDbs = "somedb";
        executedGtidReaders = networkContextResource1.getExecutedGtidReaders(null);
        Assert.assertTrue(executedGtidReaders.stream().anyMatch(e -> e instanceof DbTransactionTableGtidReader));
        Assert.assertTrue(executedGtidReaders.stream().anyMatch(e -> e instanceof TransactionTableGtidReader));
    }

    @Test(expected = Exception.class)
    public void testQueryFromDbException() {
        NetworkContextResource networkContextResource1 = new NetworkContextResource();
        networkContextResource1.applyMode = ApplyMode.db_transaction_table.getType();
        networkContextResource1.getExecutedGtidReaders(null);
    }
    class TestNetworkContextResource extends NetworkContextResource{

        @Override
        protected GtidSet queryPositionFromDb() {
            return new GtidSet(StringUtils.EMPTY);
        }
    }
}
