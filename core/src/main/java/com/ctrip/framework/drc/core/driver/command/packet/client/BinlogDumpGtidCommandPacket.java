package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_BINLOG_DUMP_GTID;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 * https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
 */
public class BinlogDumpGtidCommandPacket extends AbstractServerCommandWithHeadPacket<BinlogDumpGtidCommandPacket> {

    public static final int BINLOG_DUMP_NON_BLOCK   = 0x01;

    public static final int BINLOG_THROUGH_POSITION = 0x02;

    public static final int BINLOG_THROUGH_GTID     = 0x04;  //dump by gtid

    public long slaveServerId;

    public GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());

    public BinlogDumpGtidCommandPacket() {
        super(COM_BINLOG_DUMP_GTID.getCode());
    }

    public BinlogDumpGtidCommandPacket(byte command) {
        super(command);
    }


    public BinlogDumpGtidCommandPacket(long slaveServerId, GtidSet gtidSet) {
        super(COM_BINLOG_DUMP_GTID.getCode());
        this.slaveServerId = slaveServerId;
        this.gtidSet = gtidSet;
    }

    @Override
    public BinlogDumpGtidCommandPacket read(ByteBuf byteBuf) {
        headerPacket.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        super.write(byteBuf);
    }

    @Override
    protected byte[] getBody() throws IOException {
        return toBytes();
    }

    @Override
    public void write(IoCache ioCache) {

    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 0. [1] write command number
        out.write(getCommand());
        // 1. [2] flags
        ByteHelper.writeUnsignedShortLittleEndian(BINLOG_THROUGH_GTID, out);
        // 2. [4] server-id
        ByteHelper.writeUnsignedIntLittleEndian(slaveServerId, out);
        // 3. [4] binlog-filename-len
        ByteHelper.writeUnsignedIntLittleEndian(0, out);
        // 4. [] binlog-filename
        // skip
        // 5. [8] binlog-pos
        ByteHelper.writeUnsignedInt64LittleEndian(4, out);
        // if flags & BINLOG_THROUGH_GTID {
        byte[] bs = gtidSet.encode();
        // 6. [4] data-size
        ByteHelper.writeUnsignedIntLittleEndian(bs.length, out);
        // 7, [] data
        // [8] n_sids // 文档写的是4个字节，其实是8个字节
        // for n_sids {
        // [16] SID
        // [8] n_intervals
        // for n_intervals {
        // [8] start (signed)
        // [8] end (signed)
        // }
        // }
        out.write(bs);
        // }

        return out.toByteArray();
    }

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;
        // 2. read flags
        int flags = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;

        // 3. read gtidSet
        slaveServerId = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index += 4;

        // 4. skip file name
        index +=4;

        // 5. skip file pos
        index +=8;

        // 6. gtid length
        long length = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index +=4;

        byte[] gtidSetBytes = ByteHelper.readFixedLengthBytes(data, index, (int) length);
        gtidSet.decode(gtidSetBytes);

        // end read
    }

    public long getSlaveServerId() {
        return slaveServerId;
    }

    public GtidSet getGtidSet() {
        return gtidSet;
    }
}
