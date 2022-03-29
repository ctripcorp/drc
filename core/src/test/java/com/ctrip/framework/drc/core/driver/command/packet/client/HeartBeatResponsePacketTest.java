package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author limingdong
 * @create 2022/3/29
 */
public class HeartBeatResponsePacketTest extends AbstractCommandPacketTest {

    private HeartBeatResponsePacket heartBeatResponsePacket;

    @Before
    public void setUp() throws Exception {
        heartBeatResponsePacket = new HeartBeatResponsePacket();
    }

    @Test
    public void readwrite() throws IOException {
        heartBeatResponsePacket.write(byteBuf);
        HeartBeatResponsePacket clone = new HeartBeatResponsePacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(heartBeatResponsePacket.getBody(), clone.getBody());
        Assert.assertEquals(heartBeatResponsePacket.getBody().length, 1);
        Assert.assertEquals(heartBeatResponsePacket.getCommand(), SERVER_COMMAND.COM_HEARTBEAT_RESPONSE.getCode());
    }
}