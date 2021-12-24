package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public class PersistPostFilterTest extends AbstractFilterTest {

    private PersistPostFilter persistPostFilter;

    @Mock
    private TransactionCache transactionCache;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        persistPostFilter = new PersistPostFilter(transactionCache);
    }

    @Test
    public void doGtidFilterReleaseFalse() {
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);

        logEventWithGroupFlag.setInExcludeGroup(false);
        boolean skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
        verify(transactionCache, times(1)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), GTID);

        skip = persistPostFilter.doFilter(logEventWithGroupFlag);  //persist faked xid
        Assert.assertFalse(skip);
        verify(transactionCache, times(3)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), GTID);

        verify(gtidLogEvent, times(0)).release();
    }

    @Test
    public void doGtidFilterReleaseTrue() {
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);

        logEventWithGroupFlag.setInExcludeGroup(true);
        boolean skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
        verify(transactionCache, times(1)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        skip = persistPostFilter.doFilter(logEventWithGroupFlag);  //not persist xid
        Assert.assertTrue(skip);
        verify(transactionCache, times(2)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        verify(gtidLogEvent, times(0)).release();
    }

    @Test
    public void doXidFilterReleaseFalse() {
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        logEventWithGroupFlag.setInExcludeGroup(false);
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        logEventWithGroupFlag.setNotRelease(false);

        boolean skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
        verify(transactionCache, times(1)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);
        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(0)).release();

        logEventWithGroupFlag.setNotRelease(false);
        skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
        verify(transactionCache, times(2)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(0)).release();
    }

    @Test
    public void doXidFilterReleaseTrue() {
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        logEventWithGroupFlag.setNotRelease(false);

        boolean skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
        verify(transactionCache, times(0)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);
        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(1)).release();


        logEventWithGroupFlag.setNotRelease(false);
        logEventWithGroupFlag.setInExcludeGroup(true);
        skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
        verify(transactionCache, times(0)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(2)).release();
    }

    @Test
    public void doXidFilterReleaseTrueAndTabelFilteredTrue() {
        when(xidLogEvent.getLogEventType()).thenReturn(LogEventType.xid_log_event);
        logEventWithGroupFlag.setInExcludeGroup(true);
        logEventWithGroupFlag.setTableFiltered(true);  //first invoke and then set false
        logEventWithGroupFlag.setLogEvent(xidLogEvent);
        logEventWithGroupFlag.setNotRelease(false);

        boolean skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
        verify(transactionCache, times(1)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(0)).release();

        logEventWithGroupFlag.setNotRelease(false);
        logEventWithGroupFlag.setInExcludeGroup(true);
        skip = persistPostFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
        verify(transactionCache, times(1)).add(any(LogEvent.class));
        Assert.assertEquals(logEventWithGroupFlag.getGtid(), StringUtils.EMPTY);

        logEventWithGroupFlag.releaseEvent();
        verify(xidLogEvent, times(1)).release();
    }

}
