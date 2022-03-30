package com.ctrip.framework.drc.replicator.impl.oubound.handler;

import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.command.ServerCommandPacket;
import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.framework.drc.core.driver.command.handler.HeartBeatHandler;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mingdongli
 * 2019/9/21 下午11:22
 */
public class CommandHandlerManager extends AbstractLifecycle implements HeartBeatHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Map<SERVER_COMMAND, CommandHandler> handlers = new ConcurrentHashMap<>();

    public void addHandler(CommandHandler handler) {
        handlers.put(handler.getCommandType(), handler);
    }

    public void handle(NettyClient nettyClient, ByteBuf byteBuf) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
            byte code = (byte) (byteBuf.getByte(MySQLConstants.HEADER_PACKET_LENGTH) & 0xFF);
            SERVER_COMMAND server_command = SERVER_COMMAND.getType(code);
            if (server_command != null) {

                Class<?> clazz = server_command.getClazz();
                Constructor<?> constructor = clazz.getDeclaredConstructor(byte.class);
                constructor.setAccessible(true);
                Object objectPacket = constructor.newInstance(code);
                ServerCommandPacket packet = (ServerCommandPacket) objectPacket;
                packet.read(byteBuf);
                CommandHandler handler = getCommandHandler(server_command);
                handler.handle(packet, nettyClient);
            }

    }

    private CommandHandler getCommandHandler(SERVER_COMMAND command) {
        return handlers.get(command);
    }

    @Override
    protected void doDispose() throws Exception {
        for (Map.Entry<SERVER_COMMAND, CommandHandler> entry : handlers.entrySet()) {
            CommandHandler commandHandler = entry.getValue();
            if (commandHandler instanceof Disposable) {
                ((Disposable) commandHandler).dispose();
                logger.info("[CommandHandler] dispose for {}", entry.getKey());
            }
        }
    }

    @Override
    protected void doInitialize() throws Exception {
        for (Map.Entry<SERVER_COMMAND, CommandHandler> entry : handlers.entrySet()) {
            CommandHandler commandHandler = entry.getValue();
            if (commandHandler instanceof Initializable) {
                ((Initializable) commandHandler).initialize();
                logger.info("[CommandHandler] initialize for {}", entry.getKey());
            }
        }
    }

    @Override
    public void sendHeartBeat(Channel channel) {
        for (Map.Entry<SERVER_COMMAND, CommandHandler> entry : handlers.entrySet()) {
            CommandHandler commandHandler = entry.getValue();
            if (commandHandler instanceof HeartBeatHandler) {
                ((HeartBeatHandler) commandHandler).sendHeartBeat(channel);
            }
        }
    }
}
