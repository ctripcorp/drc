package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/10/5 下午9:48.
 */
public class ErrorPacket extends HeaderPacket {

    public byte fieldCount;

    public int errorNumber;

    public byte sqlStateMarker;

    public byte[] sqlState;

    public String message;

    @Override
    public ErrorPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        byte[] body = toBytes();
        setPacketBodyLength(body.length);
        setPacketSequenceNumber((byte) 0);
        byteBuf.writeBytes(super.toBytes());
        byteBuf.writeBytes(body);
    }

    @Override
    public void write(IoCache ioCache) {

    }

    /**
     * <pre>
     * VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1                           field_count, always = 0xff
     *  2                           errno
     *  1                           (sqlstate marker), always '#'
     *  5                           sqlstate (5 characters)
     *  n                           message
     *
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        this.fieldCount = data[0];
        index++;
        // 2. read error no.
        this.errorNumber = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
//        // 3. read marker
//        this.sqlStateMarker = data[index];
//        index++;
//        // 4. read sqlState
//        this.sqlState = ByteHelper.readFixedLengthBytes(data, index, 5);
//        index += 5;
        // 5. read message
        this.message = new String(ByteHelper.readFixedLengthBytes(data, index, data.length - index));
        // end read
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((byte) 0);
        ByteHelper.writeUnsignedShortLittleEndian(errorNumber, out);
        byte[] messageBytes = message.getBytes("UTF-8");
        out.write(messageBytes);
        return out.toByteArray();
    }

    @Override
    public String toString() {
        return "ErrorPacket [errorNumber=" + errorNumber + ", fieldCount=" + fieldCount + ", message=" + message
                + ", sqlState=" + sqlStateToString() + ", sqlStateMarker=" + (char) sqlStateMarker + "]";
    }

    private String sqlStateToString() {
        StringBuilder builder = new StringBuilder(5);
        for (byte b : this.sqlState) {
            builder.append((char) b);
        }
        return builder.toString();
    }
}
