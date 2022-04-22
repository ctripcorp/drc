package com.ctrip.framework.drc.core.driver.command.packet.applier;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_APPLIER_BINLOG_DUMP_GTID;

/**
 * who read which binlog from where
 * Created by mingdongli
 * 2019/9/21 下午9:28
 */
public class ApplierDumpCommandPacket extends AbstractServerCommandWithHeadPacket<ApplierDumpCommandPacket> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private String applierName;

    private GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());

    private int consumeType = ConsumeType.Applier.getCode();  // ConsumeType instance

    private Set<String> includedDbs = Sets.newHashSet();

    private String nameFilter = StringUtils.EMPTY;

    public ApplierDumpCommandPacket(byte command) {
        super(command);
    }

    public ApplierDumpCommandPacket(String applierName, GtidSet gtidSet) {
        this(applierName, gtidSet, Sets.newHashSet(), StringUtils.EMPTY);
    }

    public ApplierDumpCommandPacket(String applierName, GtidSet gtidSet, Set<String> includedDbs, String nameFilter) {
        super(COM_APPLIER_BINLOG_DUMP_GTID.getCode());
        this.applierName = applierName;
        this.gtidSet = gtidSet;
        this.includedDbs = includedDbs;
        this.nameFilter = nameFilter;
    }

    @Override
    public ApplierDumpCommandPacket read(ByteBuf byteBuf) {
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
        ByteHelper.writeNullTerminatedString(applierName, out);

        byte[] bs = gtidSet.encode();
        // 6. [4] data-size
        ByteHelper.writeUnsignedInt64LittleEndian(bs.length, out);
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

        ByteHelper.writeUnsignedShortLittleEndian(consumeType, out);

        // includedDbs
        int dbSize = includedDbs.size();
        ByteHelper.writeUnsignedInt64LittleEndian(dbSize, out);
        if (dbSize > 0) {
            for (String dbName : includedDbs) {
                ByteHelper.writeNullTerminatedString(dbName, out);
            }
        }

        // nameFilter
        ByteHelper.writeNullTerminatedString(nameFilter, out);

        return out.toByteArray();
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            protocol_version
     *  n (Null-Terminated String)   server_version
     *  4                            thread_id
     *  8                            scramble_buff
     *  1                            (filler) always 0x00
     *  2                            server_capabilities
     *  1                            server_language
     *  2                            server_status
     *  13                           (filler) always 0x00 ...
     *  13                           rest of scramble_buff (4.1)
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;
        // 2. read applierName
        byte[] applierBytes = ByteHelper.readNullTerminatedBytes(data, index);
        applierName = new String(applierBytes);
        index += (applierBytes.length + 1);

        // 3. read gtidSet
        long length = ByteHelper.readUnsignedLongLittleEndian(data, index);
        index += 8;
        byte[] gtidSetBytes = ByteHelper.readFixedLengthBytes(data, index, (int) length);
        gtidSet.decode(gtidSetBytes);
        gtidSet = gtidSet.clone();
        index += gtidSetBytes.length;

        consumeType = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;

        if (data.length > index) {
            long dbSize = ByteHelper.readUnsignedLongLittleEndian(data, index);
            index += 8;

            if (dbSize > 0) {
                for (int i = 0; i < dbSize; ++i) {
                    byte[] dbNameBytes = ByteHelper.readNullTerminatedBytes(data, index);
                    includedDbs.add(new String(dbNameBytes));
                    index += dbNameBytes.length + 1;
                }
            }
        }

        if (data.length > index) {
            byte[] filterBytes = ByteHelper.readNullTerminatedBytes(data, index);
            nameFilter = new String(filterBytes);
        }

        // end read
    }

    public String getApplierName() {
        return applierName;
    }

    public void setApplierName(String applierName) {
        this.applierName = applierName;
    }

    public GtidSet getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(GtidSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    public int getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(int consumeType) {
        this.consumeType = consumeType;
    }

    public Set<String> getIncludedDbs() {
        return includedDbs;
    }

    public void setIncludedDbs(Set<String> includedDbs) {
        this.includedDbs = includedDbs;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }
}
