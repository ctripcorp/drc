package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.BackupTransactionEvent;
import com.ctrip.framework.drc.replicator.store.AbstractTransactionTest;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/9/30
 */
public class TypeConvertFilterTest extends AbstractTransactionTest {

    private TypeConvertFilter typeConvertFilter = new TypeConvertFilter();

    private ITransactionEvent iTransactionEvent = new BackupTransactionEvent();

    private List<LogEvent> eventList = new ArrayList<>();

    private GtidLogEvent gtidLogEvent;

    private GtidLogEvent gtidLogEventNotModify;

    @Before
    public void setUp() throws Exception {
        gtidLogEvent = new GtidLogEvent().read(getGtidEvent());
        gtidLogEventNotModify = new GtidLogEvent().read(getGtidEvent());

        ByteBuf byteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(byteBuf);

        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();

        eventList.add(gtidLogEventNotModify);
        eventList.add(gtidLogEvent);
        eventList.add(tableMapLogEvent);
        eventList.add(new TransactionTableMarkedXidLogEvent(xidLogEvent));

        iTransactionEvent.setEvents(eventList);
    }

    @Test
    public void doFilter() {
        boolean res = typeConvertFilter.doFilter(iTransactionEvent);
        Assert.assertFalse(res);
        Assert.assertTrue(gtidLogEvent.getLogEventType() == LogEventType.drc_gtid_log_event);
        Assert.assertTrue(gtidLogEventNotModify.getLogEventType() == LogEventType.gtid_log_event);
    }
}
