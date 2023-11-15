package com.ctrip.framework.drc.core.server.common;

import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.GtidLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import io.netty.buffer.*;
import org.junit.Test;

import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

/**
 * Created by jixinwang on 2023/11/10
 */
public class EventReaderTest {

    @Test
    public void readEvent() {

        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(2);

        ByteBuf headByteBuf = initHeaderByteBuf();
        ByteBuf bodyByteBuf = initBodyByteBuf();
        compositeByteBuf.addComponents(true, headByteBuf);

    }


    private ByteBuf initHeaderByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(19);
        byte[] bytes = new byte[] {
                (byte) 0x7e, (byte) 0x6b, (byte) 0x78, (byte) 0x5d, (byte) 0x21, (byte) 0x01, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x41, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x9e, (byte) 0x2e, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }

    private ByteBuf initBodyByteBuf() {
        final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(46);
        byte[] bytes = new byte[] {
                (byte) 0x00, (byte) 0xa0, (byte) 0xa1, (byte) 0xfb, (byte) 0xb8,
                (byte) 0xbd, (byte) 0xc8, (byte) 0x11, (byte) 0xe9, (byte) 0x96, (byte) 0xa0, (byte) 0xfa, (byte) 0x16,

                (byte) 0x3e, (byte) 0x7a, (byte) 0xf2, (byte) 0xad, (byte) 0x42, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x02, (byte) 0x1e, (byte) 0x00, (byte) 0x00,

                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x1f, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x40, (byte) 0xe9, (byte) 0xd9,

                (byte) 0x34
        };
        byteBuf.writeBytes(bytes);

        return byteBuf;
    }
}