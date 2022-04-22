package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.LogEventCallBack;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.XidLogEvent;
import com.ctrip.framework.drc.replicator.MockTest;
import org.junit.Before;
import org.mockito.Mock;

/**
 * @Author limingdong
 * @create 2020/3/12
 */
public abstract class AbstractFilterTest extends MockTest {

    protected static final long EVENT_SIZE = 65L;

    protected LogEventInboundContext logEventWithGroupFlag;

    protected static final String GTID = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    @Mock
    protected GtidLogEvent gtidLogEvent;

    @Mock
    protected XidLogEvent xidLogEvent;

    @Mock
    protected LogEventCallBack callBack;

    @Before
    public void setUp() throws Exception {
        super.initMocks();
        logEventWithGroupFlag = new LogEventInboundContext(gtidLogEvent, callBack,false, false, false, "");
    }
}
