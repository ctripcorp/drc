package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.command.packet.client.RegisterSlaveCommandPacket.HOSTNAME_LENGTH;

/**
 * Created by mingdongli
 * 2019/10/17 上午11:46.
 */
public class RegisterSlaveCommandPacketTest extends AbstractCommandPacketTest {

    public static final String REPORT_HOST = "reportHostreportHostreportHostreportHostreportHostreportHostreportHostreportHost";

    public static final int REPORT_PORT = 1222;

    public static final String REPORT_USER = "reportUser";

    public static final String REPORT_PASSWD = "reportPasswd";

    public static final long SERVER_ID = 1223;

    private RegisterSlaveCommandPacket registerSlaveCommandPacket;

    @Before
    public void setUp() throws Exception {
        registerSlaveCommandPacket = new RegisterSlaveCommandPacket(REPORT_HOST, REPORT_PORT, REPORT_USER, REPORT_PASSWD, SERVER_ID);
    }

    @Test
    public void write() throws IOException {
        registerSlaveCommandPacket.write(byteBuf);
        RegisterSlaveCommandPacket clone = new RegisterSlaveCommandPacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(registerSlaveCommandPacket.getBody(), clone.getBody());

        String reportHost = clone.getReportHost();
        Assert.assertEquals(reportHost.length(), HOSTNAME_LENGTH);
        Assert.assertEquals(reportHost, REPORT_HOST.substring(0, HOSTNAME_LENGTH));
    }
}