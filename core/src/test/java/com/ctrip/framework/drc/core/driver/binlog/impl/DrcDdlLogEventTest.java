package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/3/11
 */
public class DrcDdlLogEventTest {

    private static final String SCHEMA = "drc1";

    private static final String DDL = "CREATE TABLE `drc1`.`insert1` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private DrcDdlLogEvent drcDdlLogEvent;

    @Before
    public void setUp() throws Exception {
        drcDdlLogEvent = new DrcDdlLogEvent(SCHEMA, DDL, 0, 0);
    }

    @Test
    public void read() {
        ByteBuf headByteBuf = drcDdlLogEvent.getLogEventHeader().getHeaderBuf();
        headByteBuf.readerIndex(0);

        ByteBuf payloadByteBuf =  drcDdlLogEvent.getPayloadBuf();
        payloadByteBuf.readerIndex(0);

        DrcDdlLogEvent clone = new DrcDdlLogEvent();

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(headByteBuf.readableBytes() + payloadByteBuf.readableBytes());
        compositeByteBuf.addComponents(true, headByteBuf, payloadByteBuf);

        clone.read(compositeByteBuf);

        Assert.assertEquals(clone.getSchema(), drcDdlLogEvent.getSchema());
        Assert.assertEquals(clone.getDdl(), drcDdlLogEvent.getDdl());

    }
}