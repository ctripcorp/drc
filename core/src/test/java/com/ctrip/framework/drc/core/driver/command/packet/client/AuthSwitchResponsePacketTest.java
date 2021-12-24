package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/8 上午9:09.
 */
public class AuthSwitchResponsePacketTest extends AbstractCommandPacketTest {

    private static final byte[] auth = new byte[] {0x01, 0x02, 0x03, 0x04};

    private AuthSwitchResponsePacket authSwitchResponsePacket;

    @Before
    public void setUp() {

        authSwitchResponsePacket = new AuthSwitchResponsePacket();
        authSwitchResponsePacket.setAuthData(auth);
    }

    @Test
    public void write() throws IOException {
        authSwitchResponsePacket.write(byteBuf);
        AuthSwitchResponsePacket clone = new AuthSwitchResponsePacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(authSwitchResponsePacket.getAuthData(), clone.getAuthData());
    }
}