package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.EventReaderException;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import io.netty.buffer.ByteBuf;
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

    private String registerKey;

    public ReadFilter(String registerKey) {
        this.registerKey = registerKey;
    }

    @Override
    public boolean doFilter(OutboundLogEventContext value) {
        FileChannel fileChannel = value.getFileChannel();

        EventReader.readHeader(fileChannel, headBuffer, headByteBuf);
        value.setHeadByteBuf(headByteBuf);

        LogEventType eventType = LogEventUtils.parseNextLogEventType(headByteBuf);
        value.setEventType(eventType);

        long eventSize = LogEventUtils.parseNextLogEventSize(headByteBuf);
        value.setEventSize(eventSize);

        try {
            //TODO: can remove
            if (!checkEventSize(fileChannel, eventSize)) {
                value.setCause(new EventReaderException("check event size error"));
                value.setSkipEvent(true);
            }

            if (checkPartialTransaction(fileChannel, eventSize, eventType, value.isEverSeeGtid())) {
                value.setSkipEvent(true);
            }

        } catch (IOException e) {
            value.setCause(new Exception("check event size error"));
        }

        return doNext(value, value.isSkipEvent());
    }

    private boolean checkEventSize(FileChannel fileChannel, long eventSize) throws IOException {
        if (fileChannel.position() + eventSize - eventHeaderLengthVersionGt1 > fileChannel.size()) {
            fileChannel.position(fileChannel.position() - eventHeaderLengthVersionGt1);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.read.check.size", registerKey);
            logger.warn("check event size false, size: {}", eventSize);
            return false;
        }
        return true;
    }

    // first file start with non gtid event, for example gtid in binlog.00001, and tablemap in binlog.00002
    private boolean checkPartialTransaction(FileChannel fileChannel, long eventSize, LogEventType eventType, boolean everSeeGtid) throws IOException {
        if (!everSeeGtid && !LogEventUtils.isDrcEvent(eventType)) {
            fileChannel.position(fileChannel.position() + eventSize - eventHeaderLengthVersionGt1);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.read.partial", registerKey);
            return true;
        }
        return false;
    }
}
