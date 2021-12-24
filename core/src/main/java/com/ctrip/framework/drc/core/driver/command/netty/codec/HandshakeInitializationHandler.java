package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.framework.drc.core.driver.command.packet.client.ClientAuthenticationPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.HandshakeInitializationPacket;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/9/6 上午11:39.
 */
public class HandshakeInitializationHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(HandshakeInitializationHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        if (in != null && in.readableBytes() > 0) {
            HandshakeInitializationPacket handshakePacket = new HandshakeInitializationPacket();
            handshakePacket.read(in);

            if (handshakePacket.protocolVersion != MySQLConstants.DEFAULT_PROTOCOL_VERSION) {
                return;
            }

            ClientAuthenticationPacket clientAuth = new ClientAuthenticationPacket();
            clientAuth.setCharsetNumber((byte) 33);

            NettyClientWithEndpoint nettyClientWithEndpoint = ctx.channel().attr(NettyClientFactory.KEY_CLIENT).get();
            Endpoint endpoint = nettyClientWithEndpoint.endpoint();

            clientAuth.setUsername(endpoint.getUser());
            clientAuth.setPassword(endpoint.getPassword());
            clientAuth.setServerCapabilities(handshakePacket.serverCapabilities);
            clientAuth.setDatabaseName(endpoint.getScheme());
            clientAuth.setScrumbleBuff(joinAndCreateScrumbleBuff(handshakePacket));
            clientAuth.setAuthPluginName("mysql_native_password".getBytes());

            byte[] clientAuthPkgBody = clientAuth.toBytes();
            clientAuth.setPacketBodyLength(clientAuthPkgBody.length);
            clientAuth.setPacketSequenceNumber((byte) (handshakePacket.getPacketSequenceNumber() + 1));

            ctx.channel().writeAndFlush(clientAuth);
            logger.info("handshake initialization packet received, client authentication packet was send with {}", clientAuth);
            ctx.pipeline().remove(this);

        }

    }

    private byte[] joinAndCreateScrumbleBuff(HandshakeInitializationPacket handshakePacket) {
        byte[] dst = new byte[handshakePacket.seed.length + handshakePacket.restOfScrambleBuff.length];
        System.arraycopy(handshakePacket.seed, 0, dst, 0, handshakePacket.seed.length);
        System.arraycopy(handshakePacket.restOfScrambleBuff,
                0,
                dst,
                handshakePacket.seed.length,
                handshakePacket.restOfScrambleBuff.length);
        return dst;
    }
}
