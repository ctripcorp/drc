package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by mingdongli
 * 2019/9/10 下午3:24.
 */
public class EOFPacket extends HeaderPacket {

    public byte fieldCount;

    public int warningCount;

    public int statusFlag;

    @Override
    public EOFPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                 Name
     *  -----                 ----
     *  1                     field_count, always = 0xfe
     *  2                     warning_count
     *  2                     Status Flags
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        fieldCount = data[index];
        index++;
        // 2. read warning count
        this.warningCount = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 3. read status flag
        this.statusFlag = ByteHelper.readUnsignedShortLittleEndian(data, index);
        // end read
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(5);
        out.write(this.fieldCount);
        ByteHelper.writeUnsignedShortLittleEndian(this.warningCount, out);
        ByteHelper.writeUnsignedShortLittleEndian(this.statusFlag, out);
        return out.toByteArray();
    }
}
