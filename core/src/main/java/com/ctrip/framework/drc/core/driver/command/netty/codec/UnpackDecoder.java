package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteOrder;

/**
 * Created by mingdongli
 * 2019/9/6 上午12:23.
 *
 *        3      |       1      |      N
 * body length   |    seq num   |    payload
 */
public class UnpackDecoder extends LengthFieldBasedFrameDecoder {

    //0 3 1 0
    public UnpackDecoder() {
        super(ByteOrder.LITTLE_ENDIAN, MySQLConstants.MAX_PACKET_LENGTH, MySQLConstants.HEADER_PACKET_LENGTH_FIELD_OFFSET, MySQLConstants.HEADER_PACKET_LENGTH_FIELD_LENGTH, MySQLConstants.HEADER_PACKET_NUMBER_FIELD_LENGTH, 0, true);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        return super.decode(ctx, in);
    }

}
