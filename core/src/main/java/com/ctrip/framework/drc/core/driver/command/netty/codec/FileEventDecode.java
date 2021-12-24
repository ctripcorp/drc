package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;

/**
 * netty 中lenth表示lenth字段之后的长度，event中长度表示整个event的长度，所以需要减去长度字段之前的长度
 * Created by mingdongli
 * 2019/9/24 下午4:05.
 */
public class FileEventDecode extends LengthFieldBasedFrameDecoder {

    private Logger logger = LoggerFactory.getLogger(getClass());

    //9 4 -13 0
    public FileEventDecode() {
        super(ByteOrder.LITTLE_ENDIAN, MySQLConstants.MAX_PACKET_LENGTH, MySQLConstants.EVENT_LEN_OFFSET, MySQLConstants.EVENT_LEN_LENTH, -(MySQLConstants.EVENT_LEN_OFFSET + MySQLConstants.EVENT_LEN_LENTH), 0, true);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        try {
            return super.decode(ctx, in);
        } catch (Exception e) {
            logger.warn("decode error", e);
            return null;
        }
    }

}
