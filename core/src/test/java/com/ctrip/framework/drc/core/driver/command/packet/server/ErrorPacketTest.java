package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/22 下午12:22.
 */
public class ErrorPacketTest extends AbstractCommandPacketTest {

    private ErrorPacket errorPacket;

    @Before
    public void setUp() throws Exception {
        errorPacket = new ErrorPacket();
        errorPacket.errorNumber = ResultCode.UNKNOWN_ERROR.getCode();
        errorPacket.message = ResultCode.UNKNOWN_ERROR.getMessage();
    }

    @Test
    public void read() throws IOException {
        errorPacket.write(byteBuf);
        ErrorPacket clone = new ErrorPacket();
        clone.read(byteBuf);
        Assert.assertEquals(errorPacket.message, clone.message);
        Assert.assertEquals(errorPacket.errorNumber, clone.errorNumber);
    }
}