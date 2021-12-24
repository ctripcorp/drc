package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/4/24
 */
public class EventReleaseFilterTest extends AbstractFilterTest {

    private EventReleaseFilter eventReleaseFilter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        eventReleaseFilter = new EventReleaseFilter();
        when(gtidLogEvent.getLogEventType()).thenReturn(LogEventType.gtid_log_event);
    }

    @Test
    public void doFilterReleaseFalse() {
        logEventWithGroupFlag.setNotRelease(false);
        eventReleaseFilter.doFilter(logEventWithGroupFlag);
        verify(gtidLogEvent, times(1)).release();
    }

    @Test
    public void doFilterReleaseTrue() {
        logEventWithGroupFlag.setNotRelease(true);
        eventReleaseFilter.doFilter(logEventWithGroupFlag);
        verify(gtidLogEvent, times(0)).release();
    }
}