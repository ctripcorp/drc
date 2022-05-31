package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_REGISTER_SLAVE;

/**
 * Created by mingdongli
 * 2019/9/10 下午3:59.
 */
public class RegisterSlaveCommandPacket extends AbstractServerCommandWithHeadPacket<RegisterSlaveCommandPacket> {

    public static final int HOSTNAME_LENGTH = 60;

    public String reportHost;

    public int reportPort;

    public String reportUser;

    public String reportPasswd;

    public long serverId;

    public RegisterSlaveCommandPacket() {
        super(COM_REGISTER_SLAVE.getCode());
    }

    public RegisterSlaveCommandPacket(String reportHost, int reportPort, String reportUser, String reportPasswd, long serverId) {
        super(COM_REGISTER_SLAVE.getCode());
        if (StringUtils.isNotBlank(reportHost) && reportHost.length() > HOSTNAME_LENGTH) {
            reportHost = reportHost.substring(0, HOSTNAME_LENGTH);
        }
        this.reportHost = reportHost;
        this.reportPort = reportPort;
        this.reportUser = reportUser;
        this.reportPasswd = reportPasswd;
        this.serverId = serverId;
    }

    @Override
    public RegisterSlaveCommandPacket read(ByteBuf byteBuf) {
        headerPacket.read(byteBuf);
        fromBytes(getBody(byteBuf));
        return this;
    }

    @Override
    public void write(ByteBuf byteBuf) throws IOException {
        super.write(byteBuf);
    }

    @Override
    protected byte[] getBody() {
        return toBytes();
    }

    @Override
    public void write(IoCache ioCache) {

    }

    public byte[] toBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        ByteHelper.writeUnsignedIntLittleEndian(serverId, out);
        out.write((byte) reportHost.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportHost.getBytes(), reportHost.getBytes().length, out);
        out.write((byte) reportUser.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportUser.getBytes(), reportUser.getBytes().length, out);
        out.write((byte) reportPasswd.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportPasswd.getBytes(), reportPasswd.getBytes().length, out);
        ByteHelper.writeUnsignedShortLittleEndian(reportPort, out);
        ByteHelper.writeUnsignedIntLittleEndian(0, out);// Fake
        // rpl_recovery_rank
        ByteHelper.writeUnsignedIntLittleEndian(0, out);// master id
        return out.toByteArray();
    }

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;
        // 2. read serverId
        serverId = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index += 4;

        int length = data[index++];
        byte[] reportHostByte = ByteHelper.readFixedLengthBytes(data, index, length);
        reportHost = new String(reportHostByte);
        index += length;

        length = data[index++];
        byte[] reportUserByte = ByteHelper.readFixedLengthBytes(data, index, length);
        reportUser = new String(reportUserByte);
        index += length;

        length = data[index++];
        byte[] reportPasswdByte = ByteHelper.readFixedLengthBytes(data, index, length);
        reportPasswd = new String(reportPasswdByte);
        index += length;

        reportPort = ByteHelper.readUnsignedShortLittleEndian(data, index);
    }

    @VisibleForTesting
    public String getReportHost() {
        return reportHost;
    }
}
