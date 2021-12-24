package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by mingdongli
 * 2019/9/6 下午4:18.
 */
public class PackageEncoder extends MessageToByteEncoder<Packet> {

    @Override
	protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) throws Exception {
        msg.write(out);
	}

}
