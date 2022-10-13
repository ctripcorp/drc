package com.ctrip.framework.drc.core.driver.command.packet.monitor;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-11
 */
public class DelayMonitorCommandPacketTest extends AbstractCommandPacketTest {

    private static final String DC_NAME = "dc name test";

    private static final String CLUSTER_NAME = "cluster name test";

    private static final String REGION_NAME = "region name test";

    private DelayMonitorCommandPacket delayMonitorCommandPacket;

    @Before
    public void setup() {
        delayMonitorCommandPacket = new DelayMonitorCommandPacket(DC_NAME, CLUSTER_NAME, REGION_NAME);
    }

    @Test
    public void write() throws IOException {
        delayMonitorCommandPacket.write(byteBuf);
        DelayMonitorCommandPacket clone  = new DelayMonitorCommandPacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(delayMonitorCommandPacket.getBody(), clone.getBody());
        Assert.assertEquals(clone.getRegion(), REGION_NAME);
    }
}
