package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.command.packet.AbstractCommandPacketTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/17 上午11:33.
 */
public class QueryCommandPacketTest extends AbstractCommandPacketTest {

    private static final String QUERY_STRING = "mysql query test";

    private QueryCommandPacket queryCommandPacket;

    @Before
    public void setUp() throws Exception {
        queryCommandPacket = new QueryCommandPacket(QUERY_STRING);
    }

    @Test
    public void write() throws IOException {
        queryCommandPacket.write(byteBuf);
        QueryCommandPacket clone = new QueryCommandPacket();
        clone.read(byteBuf);
        Assert.assertArrayEquals(queryCommandPacket.getBody(), clone.getBody());
    }
}