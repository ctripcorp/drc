package com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
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
 * @create 2021/11/24
 */
public class DdlIndexFilterTest extends AbstractTransactionTest {

    private DdlIndexFilter ddlIndexFilter = new DdlIndexFilter();

    private ITransactionEvent iTransactionEvent = new BackupTransactionEvent();

    private List<LogEvent> eventList = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);

        byteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(byteBuf);

        DrcDdlLogEvent drcDdlLogEvent = getDrcDdlLogEvent();
        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();

        eventList.add(new FilterLogEvent());
        eventList.add(gtidLogEvent);
        eventList.add(drcDdlLogEvent);
        eventList.add(tableMapLogEvent);
        eventList.add(xidLogEvent);

        iTransactionEvent.setEvents(eventList);
    }

    @Test
    public void doFilter() {
        boolean res = ddlIndexFilter.doFilter(iTransactionEvent);
        Assert.assertEquals(res, !iTransactionEvent.passFilter());
        Assert.assertTrue(((TransactionEvent)iTransactionEvent).isDdl());
    }
}