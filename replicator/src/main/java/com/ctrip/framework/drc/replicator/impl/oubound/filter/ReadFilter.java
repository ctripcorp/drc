package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.SizeNotEnoughException;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;

/**
 * Created by jixinwang on 2023/10/12
 */
public class ReadFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

    private ByteBuffer headBuffer = ByteBuffer.allocateDirect(eventHeaderLengthVersionGt1);

    private ByteBuf headByteBuf = Unpooled.wrappedBuffer(headBuffer);

    private CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(2);

    private String registerKey;

    public ReadFilter(String registerKey) {
        this.registerKey = registerKey;
        this.compositeByteBuf.addComponent(true, headByteBuf);
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        FileChannel fileChannel = value.getFileChannel();

        // if read header fail, restore position
        boolean readHeaderComplete = EventReader.readHeader(fileChannel, headBuffer, headByteBuf);
        if (!readHeaderComplete) {
            this.restore(value, fileChannel);
            return doNext(value, value.isSkipEvent());
        }

        value.setCompositeByteBuf(compositeByteBuf);

        LogEventType eventType = LogEventUtils.parseNextLogEventType(headByteBuf);
        value.setEventType(eventType);

        long eventSize = LogEventUtils.parseNextLogEventSize(headByteBuf);
        value.setEventSize(eventSize);

        // if event not complete yet, restore position
        if (!checkEventSize(value)) {
            this.restore(value, fileChannel);
        }
        return doNext(value, value.isSkipEvent());
    }

    private void restore(OutboundLogEventContext value, FileChannel fileChannel) {
        try {
            fileChannel.position(value.getFileChannelPos());
            value.setCause(new SizeNotEnoughException("check event size error"));
            value.setSkipEvent(true);
        } catch (IOException e) {
            logger.error("check event size error:", e);
            value.setCause(e);
            value.setSkipEvent(true);
        }
    }

    private boolean checkEventSize(OutboundLogEventContext value) {
        if (value.getFileChannelPos() + value.getEventSize() > value.getFileChannelSize()) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.read.check.size", registerKey);
            logger.warn("check event size false, size: {}", value.getEventSize());
            return false;
        }
        return true;
    }

    @Override
    public void release() {
        EventReader.releaseByteBuf(compositeByteBuf);
        logger.info("read filter release compositeByteBuf for: {}", registerKey);
    }
}
