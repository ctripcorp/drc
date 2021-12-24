package com.ctrip.framework.drc.core.driver.command.netty.codec;

import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactory;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.NettyClientWithEndpoint;
import com.ctrip.framework.drc.core.driver.command.packet.client.AuthSwitchResponsePacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.AuthSwitchRequestMoreDataPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.AuthSwitchRequestPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ErrorPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ServerResultPackage;
import com.ctrip.framework.drc.core.driver.util.MySQLPasswordEncrypter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by mingdongli
 * 2019/9/8 上午9:51.
 */
public class AuthenticateResultHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private Logger logger = LoggerFactory.getLogger(AuthenticateResultHandler.class);

    private boolean isSha2Password = false;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ServerResultPackage serverResultPackage = new ServerResultPackage();
        serverResultPackage.read(in);
        byte[] body = serverResultPackage.getBody();
        logger.info("authenticateresulthandler {}", body[0]);

        if (!isSha2Password && (body[0] == -2 || body[0] == 1)) {
            byte[] authData;
            String pluginName = null;
            int seqNum = serverResultPackage.getPacketSequenceNumber();
            if (body[0] == 1) {
                AuthSwitchRequestMoreDataPacket packet = new AuthSwitchRequestMoreDataPacket();
                packet.fromBytes(body);
                authData = packet.authData;
            } else {
                AuthSwitchRequestPacket packet = new AuthSwitchRequestPacket();
                packet.fromBytes(body);
                authData = packet.authData;
                pluginName = packet.authName;
            }

            byte[] encryptedPassword = null;
            NettyClientWithEndpoint nettyClientWithEndpoint = ctx.channel().attr(NettyClientFactory.KEY_CLIENT).get();
            Endpoint endpoint = nettyClientWithEndpoint.endpoint();
            if (pluginName != null && "mysql_native_password".equals(pluginName)) {
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scramble411(endpoint.getPassword().getBytes(), authData);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            } else if (pluginName != null && "caching_sha2_password".equals(pluginName)) {
                isSha2Password = true;
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scrambleCachingSha2(endpoint.getPassword().getBytes(), authData);
                } catch (DigestException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            }
            assert encryptedPassword != null;
            AuthSwitchResponsePacket responsePacket = new AuthSwitchResponsePacket();
            responsePacket.setAuthData(encryptedPassword);
            byte[] auth = responsePacket.toBytes();

            responsePacket.setPacketBodyLength(auth.length);
            responsePacket.setPacketSequenceNumber((byte) (seqNum + 1));
            ctx.channel().writeAndFlush(responsePacket);
            logger.info("auth switch response packet is sent out for {} with plugin {}:{}", endpoint, pluginName, encryptedPassword);
            return;
        }

        if(isSha2Password) {
            if (body[0] == 0x01 && body[1] == 0x04) {
                // password auth failed
                throw new IOException("caching_sha2_password Auth failed");
            }
            isSha2Password = false;
            logger.info("caching_sha2_password Auth success with body[0]:{}, body[1]:{}", body[0], body[1]);
            return;
        }

        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket errorPacket = new ErrorPacket();
                errorPacket.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + errorPacket.message);
            } else {
                throw new IOException("unexpected packet with field_count=" + body[0]);
            }
        }
        NettyClientWithEndpoint nettyClientWithEndpoint = ctx.channel().attr(NettyClientFactory.KEY_CLIENT).get();
        nettyClientWithEndpoint.getCountDownLatch().countDown();
        logger.info("count down for [AUTHENTICATION] {}", nettyClientWithEndpoint.endpoint());
        ctx.pipeline().remove(this);
    }

}
