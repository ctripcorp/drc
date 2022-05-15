package com.ctrip.framework.drc.core.driver.binlog.header;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.Packet;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by @author zhuYongMing on 2019/9/6.
 * see https://dev.mysql.com/doc/internals/en/rows-event.html
 */
public class RowsEventPostHeader implements Packet<RowsEventPostHeader> {

    private long tableId;

    private int flags;

    private int extraDataLength;

    private ByteBuf extraData;

    @Override
    public RowsEventPostHeader read(ByteBuf byteBuf) {

        try {
            final long low4 = byteBuf.readUnsignedIntLE();
            final long high2 = byteBuf.readUnsignedShortLE();
            // rows event post-header length is 10 instead of 6, so tableId length is 6 bytes
            this.tableId = low4 | (high2 << 32); // 6bytes
            this.flags = byteBuf.readUnsignedShortLE(); // 2bytes

            // if binlog version = 2, rows event has extra data, now binlog version = 2, hardcode
            this.extraDataLength = byteBuf.readUnsignedShortLE(); // 2bytes
            if (extraDataLength > 2) {
                extraData = byteBuf.readSlice(extraDataLength - 2);  // string.var_length bytes
            }

            // validate decode result
            validate();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;

    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {

    }

    public void payloadToBytes(ByteArrayOutputStream out) {
        // tableId, 6 bytes
        ByteHelper.writeUnsignedInt48LittleEndian(tableId, out);

        // flag, 2 bytes
        ByteHelper.writeUnsignedShortLittleEndian(flags, out);

        // data length, 2 bytes
        ByteHelper.writeUnsignedShortLittleEndian(extraDataLength, out);

        // extra data
        if (extraDataLength > 2) {
            out.writeBytes(extraData.array());
        }
    }

    @Override
    public void write(IoCache ioCache) {

    }

    private void validate() {

    }

    public long getTableId() {
        return tableId;
    }

    public int getFlags() {
        return flags;
    }

    public int getExtraDataLength() {
        return extraDataLength;
    }

    public ByteBuf getExtraData() {
        return extraData;
    }
}
