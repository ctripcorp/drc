package com.ctrip.framework.drc.core.driver.command.packet;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.Packet;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/6 下午1:05.
 */
public class HeaderPacket implements Packet<HeaderPacket> {

    private byte packetSequenceNumber;

    private int  packetBodyLength;

    public byte getPacketSequenceNumber() {
        return packetSequenceNumber;
    }

    public void setPacketSequenceNumber(byte packetSequenceNumber) {
        this.packetSequenceNumber = packetSequenceNumber;
    }

    public int getPacketBodyLength() {
        return packetBodyLength;
    }

    public void setPacketBodyLength(int packetBodyLength) {
        this.packetBodyLength = packetBodyLength;
    }

    public byte[] getBody(ByteBuf in) {
        byte[] dst = new byte[getPacketBodyLength()];
        in.readBytes(dst, 0, getPacketBodyLength());
        return dst;
    }

    @Override
    public HeaderPacket read(ByteBuf byteBuf) {
        this.packetBodyLength = (byteBuf.readByte() & 0xFF) | ((byteBuf.readByte() & 0xFF) << 8) | ((byteBuf.readByte() & 0xFF) << 16);
        this.setPacketSequenceNumber(byteBuf.readByte());
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        byteBuf.writeBytes(toBytes());
    }

    /**
     * little-endian byte order
     */
    public byte[] toBytes() throws IOException {
        byte[] data = new byte[4];
        data[0] = (byte) (packetBodyLength & 0xFF);
        data[1] = (byte) (packetBodyLength >>> 8);
        data[2] = (byte) (packetBodyLength >>> 16);
        data[3] = getPacketSequenceNumber();
        return data;
    }

    @Override
    public void write(IoCache ioCache) {

    }

    @Override
    public String toString() {
        return "HeaderPacket{" +
                "packetSequenceNumber=" + packetSequenceNumber +
                ", packetBodyLength=" + packetBodyLength +
                '}';
    }
}
