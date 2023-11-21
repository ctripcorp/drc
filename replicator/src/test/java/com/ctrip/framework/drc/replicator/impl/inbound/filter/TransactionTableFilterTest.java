package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.table_map_log_event;

/**
 * Created by jixinwang on 2022/2/20
 */
public class TransactionTableFilterTest extends AbstractFilterTest {

    private TransactionTableFilter transactionTableFilter;

    private InboundLogEventContext value;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transactionTableFilter = new TransactionTableFilter();
    }

    @Test
    public void doFilterTrue() throws IOException {
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "drcmonitordb", "gtid_executed", Lists.newArrayList(), null, table_map_log_event, 0
        );
        value = new InboundLogEventContext(tableMapLogEvent, null, new TransactionFlags(), GTID);

        transactionTableFilter.doFilter(value);
        Assert.assertTrue(value.isInExcludeGroup());
        Assert.assertTrue(value.isTransactionTableRelated());
    }

    @Test
    public void doFilterFalse() throws IOException {
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "drcmonitordb", "delaymonitor", Lists.newArrayList(), null, table_map_log_event, 0
        );
        value = new InboundLogEventContext(tableMapLogEvent, null,new TransactionFlags(), GTID);

        transactionTableFilter.doFilter(value);
        Assert.assertFalse(value.isInExcludeGroup());
        Assert.assertFalse(value.isTransactionTableRelated());
    }

    @Test
    public void doFilterDrcWriteFilter() throws IOException {
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent(
                1L, 813, 123, "drcmonitordb", "drc_write_filter", Lists.newArrayList(), null, table_map_log_event, 0
        );
        value = new InboundLogEventContext(tableMapLogEvent, null, new TransactionFlags(), GTID);

        transactionTableFilter.doFilter(value);
        Assert.assertTrue(value.isInExcludeGroup());
        Assert.assertTrue(value.isTransactionTableRelated());
    }

    @Test
    public void doFilterGtidLogEvent() throws IOException {
        value = new InboundLogEventContext(gtidLogEvent, null,new TransactionFlags(), GTID);

        transactionTableFilter.doFilter(value);
        Assert.assertFalse(value.isInExcludeGroup());
        Assert.assertFalse(value.isTransactionTableRelated());
    }
}
