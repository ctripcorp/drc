package com.ctrip.framework.drc.core.driver.binlog.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

/**
 * for event send through tcp
 * @Author limingdong
 * @create 2022/6/9
 */
public interface LogEventMerger {

    default List<ByteBuf> mergeByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        List<ByteBuf> res = new ArrayList<>(1);
        headByteBuf.readerIndex(0);
        payloadBuf.readerIndex(0);
        res.add(PooledByteBufAllocator.DEFAULT.compositeDirectBuffer().addComponents(true, headByteBuf, payloadBuf));
        return res;
    }
}
