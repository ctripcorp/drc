package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/10 上午11:53.
 */
public class ResultSetHeaderPacket extends HeaderPacket {

    private long columnCount;

    private long extra;

    @Override
    public ResultSetHeaderPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        try {
            fromBytes(getBody(byteBuf));
        } catch (IOException e) {
            return null;
        }
        return this;
    }

    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        byte[] colCountBytes = ByteHelper.readBinaryCodedLengthBytes(data, index);
        columnCount = ByteHelper.readLengthCodedBinary(colCountBytes, index);
        index += colCountBytes.length;
        if (index < data.length - 1) {
            extra = ByteHelper.readLengthCodedBinary(data, index);
        }
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public long getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(long columnCount) {
        this.columnCount = columnCount;
    }

    public long getExtra() {
        return extra;
    }

    public void setExtra(long extra) {
        this.extra = extra;
    }

    public String toString() {
        return "ResultSetHeaderPacket [columnCount=" + columnCount + ", extra=" + extra + "]";
    }

}
