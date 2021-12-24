package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.ITransactionEvent;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.server.common.Filter;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.EventTransactionCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Created by mingdongli
 * 2019/9/18 下午2:35.
 */
public class FilePersistenceEventStoreTest extends AbstractTransactionTest{

    private FilePersistenceEventStore eventStore;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private Filter<ITransactionEvent> filterChain;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        System.setProperty(SystemConfig.REPLICATOR_FILE_LIMIT, String.valueOf(1024 * 2));
        eventStore = new FilePersistenceEventStore(schemaManager, DESTINATION);
        fileManager = eventStore.getFileManager();

        eventStore.initialize();
        eventStore.start();

        transactionCache = new EventTransactionCache(eventStore, filterChain);
        transactionCache.initialize();
        transactionCache.start();
    }

    @After
    public void tearDown() throws Exception {
        eventStore.stop();
        eventStore.dispose();
    }

    @Test
    public void testWriteByteBuf() throws Exception {
        writeTransactionThroughTransactionCache();
        GtidSet gtidSet = fileManager.getExecutedGtids();
        Assert.assertEquals(gtidSet.add(GTID), false);
    }

}
