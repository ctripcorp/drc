package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_index_log_event;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;

/**
 * @Author limingdong
 * @create 2020/9/3
 */
public class DrcIndexLogEvent extends AbstractLogEvent {

    private static final Logger logger = LoggerFactory.getLogger(DrcIndexLogEvent.class);

    public static final int FIX_SIZE = 1024 * 2;

    private List<Long> indices;

    private List<Long> notRevisedIndices;

    public DrcIndexLogEvent() {
    }

    public DrcIndexLogEvent(List<Long> indices, List<Long> notRevisedIndices, int serverId, long currentEventStartPosition) {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeIndices(out, indices);
        writeIndices(out, notRevisedIndices);

        byte[] indexBytes = out.toByteArray();

        final int payloadLength = indexBytes.length;

        int padding = FIX_SIZE - (eventHeaderLengthVersionGt1 + payloadLength);

        if (padding < 0) {
            throw new RuntimeException("indices overflow");
        }

        setLogEventHeader(new LogEventHeader(drc_index_log_event.getType(), serverId, FIX_SIZE, currentEventStartPosition + FIX_SIZE, LOG_EVENT_IGNORABLE_F));

        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(padding + payloadLength);
        payloadByteBuf.writeBytes(indexBytes);
        payloadByteBuf.writerIndex(padding + payloadLength);
        setPayloadBuf(payloadByteBuf);

        this.indices = indices;
        this.notRevisedIndices = notRevisedIndices;
    }

    @Override
    public DrcIndexLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        long size = payloadBuf.readLongLE();
        indices = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            indices.add(payloadBuf.readLongLE());
        }

        try {
            notRevisedIndices = new ArrayList<>();
            size = payloadBuf.readLongLE();
            logger.info("notRevisedIndices size is {}", size);
            if (size > 0 && size <= 11) {
                for (int i = 0; i < size; ++i) {
                    notRevisedIndices.add(payloadBuf.readLongLE());
                }
            }
        } catch (Exception e) {
            logger.error("[Parse] error", e);
        }
        return this;
    }

    private void writeIndices(ByteArrayOutputStream out, List<Long> indices) {
        int size = indices.size();
        ByteHelper.writeUnsignedInt64LittleEndian(size, out);
        for (long index : indices) {
            ByteHelper.writeUnsignedInt64LittleEndian(index, out);
        }
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public List<Long> getIndices() {
        return indices;
    }

    public List<Long> getNotRevisedIndices() {
        return notRevisedIndices;
    }
}
