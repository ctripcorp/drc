package com.ctrip.framework.drc.core.driver.binlog.util;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yongnian
 * @create 2024/10/16 20:08
 */
public class LogEventMockUtils {


    public static String eventToHex(LogEvent<?> logEvent) {
        List<Byte> list = getBytes(logEvent);
        StringBuilder sb = new StringBuilder();

        sb.append("byte[] bytes = new byte[] {\n");
        for (Byte b : list.subList(0, 19)) {
            sb.append(String.format("(byte) 0x%02x, ", b));
        }
        sb.append("\n");
        List<Byte> body = list.subList(19, list.size());
        List<List<Byte>> partition = Lists.partition(body, 16);
        for (List<Byte> bytes : partition) {
            for (Byte b : bytes) {
                sb.append(String.format("(byte) 0x%02X, ", b));
            }
            sb.append("\n");
        }


        sb.append("};");

        return sb.toString();
    }

    public static List<Byte> getBytes(LogEvent<?> logEvent) {
        logEvent.getPayloadBuf().readerIndex(0);
        logEvent.getLogEventHeader().getHeaderBuf().readerIndex(0);
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, logEvent.getLogEventHeader().getHeaderBuf(), logEvent.getPayloadBuf());

        List<Byte> list = new ArrayList<>();
        for (int i = 0; i < compositeByteBuf.numComponents(); i++) {
            compositeByteBuf.component(i).forEachByte(b -> {
                list.add(b);
                return true;
            });
        }
        return list;
    }

    public static void loadEventFromBytes(LogEvent<?> logEvent, byte[] bytes) {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(bytes.length);
        byteBuf.writeBytes(bytes);

        logEvent.read(byteBuf);
    }

    @Test
    public void testEventToHex() {
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        filterLogEvent.encode("drc1", "UNKNONW",10,5,101);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());

        String hex = eventToHex(filterLogEvent);
        System.out.println(hex);
    }

    @Test
    public void testLoad(){
        byte[] bytes = {
                (byte) 0xcc, (byte) 0xad, (byte) 0x0f, (byte) 0x67, (byte) 0x6d, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x28, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x04, (byte) 0x64, (byte) 0x72, (byte) 0x63, (byte) 0x31, (byte) 0x65, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x07, (byte) 0x75, (byte) 0x6E, (byte) 0x6B, (byte) 0x6E, (byte) 0x6F, (byte) 0x77,
                (byte) 0x6E, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
        };
        FilterLogEvent filterLogEvent = new FilterLogEvent();
        loadEventFromBytes(filterLogEvent,bytes);
        Assert.assertEquals("drc1", filterLogEvent.getSchemaName());
        Assert.assertEquals(FilterLogEvent.UNKNOWN, filterLogEvent.getLastTableName());
        Assert.assertEquals(101, filterLogEvent.getNextTransactionOffset());
        Assert.assertEquals(0,filterLogEvent.getEventCount());
        Assert.assertNull(filterLogEvent.getRowsEventCount());
    }


}
