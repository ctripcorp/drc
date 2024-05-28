package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import io.netty.buffer.ByteBuf;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * Created by @author zhuYongMing on 2019/9/14.
 */
public class FormatDescriptionLogEvent extends AbstractLogEvent {

    private int binlogVersion;

    private String mysqlServerVersion;

    private long binlogCreateTimestamp;

    private int eventHeaderLength;

    private int[] eventPostHeaderLengths;

    private Long checksum;


    @Override
    public FormatDescriptionLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        this.binlogVersion = payloadBuf.readUnsignedShortLE(); // 2bytes

        int effectiveLength = 0;
        byte[] mysqlServerVersionBytes = new byte[50];
        for (int i = 0; i < 50; i++) {
            final byte aByte = payloadBuf.readByte();
            if (aByte != '\0') { // != 0, is effective data
                effectiveLength++;
                mysqlServerVersionBytes[i] = aByte;
            }
        }
        this.mysqlServerVersion = new String(mysqlServerVersionBytes, 0, effectiveLength, ISO_8859_1); //50bytes

        this.binlogCreateTimestamp = payloadBuf.readUnsignedIntLE();
        this.eventHeaderLength = payloadBuf.readUnsignedByte();

        int eventCount = payloadBuf.writerIndex() - payloadBuf.readerIndex() - 4;
        eventPostHeaderLengths = new int[eventCount];
        for (int i = 0; i < eventCount; i++) {
            eventPostHeaderLengths[i] = payloadBuf.readUnsignedByte();
        }

        this.checksum = readChecksumIfPossible(payloadBuf); // 4bytes

        validate();
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    private void validate() {
        // TODO: 2019/9/14 eventHeaderLength 19, eventPostHeaderLengths each one , binlogVersion 4, mysqlServerVersion 7.7.22+
    }

    public int getBinlogVersion() {
        return binlogVersion;
    }

    public String getMysqlServerVersion() {
        return mysqlServerVersion;
    }

    public long getBinlogCreateTimestamp() {
        return binlogCreateTimestamp;
    }

    public int getEventHeaderLength() {
        return eventHeaderLength;
    }

    public int[] getEventPostHeaderLengths() {
        return eventPostHeaderLengths;
    }

    public Long getChecksum() {
        return checksum;
    }
}
