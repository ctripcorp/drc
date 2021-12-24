package com.ctrip.framework.drc.core.driver;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface Packet<V extends Packet> {


    /**
     * @return V if read completely, else return null
     */
    V read(ByteBuf byteBuf);

    void write(ByteBuf byteBuf) throws IOException;

    /**
     * write current logevent to iocache
     *
     * @param ioCache
     */
    void write(IoCache ioCache);
}
