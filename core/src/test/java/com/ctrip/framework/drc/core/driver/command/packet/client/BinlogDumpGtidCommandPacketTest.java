package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/8 上午9:28.
 */
public class BinlogDumpGtidCommandPacketTest extends AbstractCommandPacketTest {

    private static final int SLAVE_ID = 1222;

    private BinlogDumpGtidCommandPacket binlogDumpGtidCommandPacket;

    @Before
    public void setUp() throws Exception {
        gtidSet = new GtidSet(GTID_SET);
        binlogDumpGtidCommandPacket = new BinlogDumpGtidCommandPacket(SLAVE_ID, gtidSet);
    }

    @Test
    public void read() throws IOException {
        binlogDumpGtidCommandPacket.write(byteBuf);
        BinlogDumpGtidCommandPacket clone = new BinlogDumpGtidCommandPacket();
        clone.read(byteBuf);
        Assert.assertEquals(binlogDumpGtidCommandPacket.slaveServerId, clone.slaveServerId);
    }
}