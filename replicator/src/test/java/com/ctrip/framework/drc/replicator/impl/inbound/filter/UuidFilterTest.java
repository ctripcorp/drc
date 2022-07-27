package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.driver.binlog.impl.DrcUuidLogEvent;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

/**
 * @Author limingdong
 * @create 2020/3/12
 */
public class UuidFilterTest extends AbstractFilterTest {

    private static final String GTID = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    private static final String GTID_SKIP = "34027356-0d03-11ea-a2f0-c6a9fbf1c3fe:1";

    private static final String UUID_1 = "56027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private static final String UUID_2 = "12027356-0d03-11ea-a2f0-c6a9fbf1c3fe";

    private UuidFilter uuidFilter;

    private DrcUuidLogEvent drcUuidLogEvent;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        Set<UUID> uuidSet = Sets.newHashSet();
        uuidSet.add(UUID.fromString(UUID_1));
        uuidFilter = new UuidFilter(uuidSet);
    }

    @Test
    public void doFilterFalse() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_1));
        when(gtidLogEvent.getGtid()).thenReturn(GTID);
        boolean skip = uuidFilter.doFilter(logEventWithGroupFlag);
        Assert.assertFalse(skip);
    }

    @Test
    public void doFilterTrue() {
        when(gtidLogEvent.getServerUUID()).thenReturn(UUID.fromString(UUID_2));
        when(gtidLogEvent.getGtid()).thenReturn(GTID_SKIP);
        boolean skip = uuidFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);
    }

    @Test
    public void testFilterDrcUuidEvent() {
        Set<String> uuids = Sets.newHashSet();
        uuids.add(UUID.randomUUID().toString());
        uuids.add(UUID.randomUUID().toString());
        drcUuidLogEvent = new DrcUuidLogEvent(uuids, 0, 10);

        InboundLogEventContext logEventWithGroupFlag = new InboundLogEventContext(drcUuidLogEvent, null, false, false, false, "");
        boolean skip = uuidFilter.doFilter(logEventWithGroupFlag);
        Assert.assertTrue(skip);

        Assert.assertNotEquals(uuids, uuidFilter.getWhiteList());
        Assert.assertTrue(uuidFilter.getWhiteList().size() == uuids.size() + 1);  //UUID_1

        uuids.add(UUID_1);

        for (String uuid : uuids) {
            Assert.assertTrue(uuidFilter.getWhiteList().contains(UUID.fromString(uuid)));
        }

    }
}
