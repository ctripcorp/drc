package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/11/6 上午9:41.
 */
public class HeartBeatPacketTest extends AbstractCommandPacketTest {

    private HeartBeatPacket heartBeatPacket;

    @Before
    public void setUp() throws Exception {
        heartBeatPacket = new HeartBeatPacket();
    }

    @Test
    public void readwrite() throws IOException {
        heartBeatPacket.write(byteBuf);
        HeartBeatPacket clone = new HeartBeatPacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(heartBeatPacket.getBody(), clone.getBody());
        Assert.assertEquals(heartBeatPacket.getBody().length, 0);
    }
}