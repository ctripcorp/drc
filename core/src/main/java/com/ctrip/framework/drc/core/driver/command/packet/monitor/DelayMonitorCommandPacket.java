package com.ctrip.framework.drc.core.driver.command.packet.monitor;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * (Request timestamp in commandHandler) by provided srcIp and destIp
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-02
 */
public class DelayMonitorCommandPacket extends AbstractServerCommandWithHeadPacket<DelayMonitorCommandPacket> {

    /**
     * specify console's(src) and replicator's(dest) ip to request timestamp
     */
    private String dcName;

    private String region = StringUtils.EMPTY;

    private String clusterName;

    public DelayMonitorCommandPacket() { super(SERVER_COMMAND.COM_DELAY_MONITOR.getCode()); }

    public DelayMonitorCommandPacket(byte command) { super(command); }

    public DelayMonitorCommandPacket(String dcName, String clusterName, String region) {
        super(SERVER_COMMAND.COM_DELAY_MONITOR.getCode());
        this.dcName = dcName;
        this.clusterName = clusterName;
        this.region = region;
    }

    @Override
    public DelayMonitorCommandPacket read(ByteBuf byteBuf) {
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

    public byte[] toBytes() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 0. [1] write command number
        out.write(getCommand());

        // 1. [2] flags
        byte[] dcNameBytes = dcName.getBytes();
        int dcNameBytesLength = dcNameBytes.length;
        out.write((byte) dcNameBytesLength);
        ByteHelper.writeFixedLengthBytesFromStart(dcNameBytes, dcNameBytesLength, out);

        byte[] clusterNameBytes = clusterName.getBytes();
        int clusterNameBytesLength = clusterNameBytes.length;
        out.write((byte) clusterNameBytesLength);
        ByteHelper.writeFixedLengthBytesFromStart(clusterNameBytes, clusterNameBytesLength, out);

        byte[] regionBytes = region.getBytes();
        int regionBytesLength = regionBytes.length;
        out.write((byte) regionBytesLength);
        ByteHelper.writeFixedLengthBytesFromStart(regionBytes, regionBytesLength, out);

        return out.toByteArray();
    }

    private void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;

        // 2. read dcName
        int length = data[index++];
        byte[] dcNameByte = ByteHelper.readFixedLengthBytes(data, index, length);
        dcName = new String(dcNameByte);
        index += length;

        // 3. read clusterName
        length = data[index++];
        byte[] clusterNameByte = ByteHelper.readFixedLengthBytes(data, index, length);
        clusterName = new String(clusterNameByte);
        index += length;

        // 4. read region
        if (data.length > index) {
            length = data[index++];
            byte[] regionByte = ByteHelper.readFixedLengthBytes(data, index, length);
            region = new String(regionByte);
            index += length;
        }
    }

    public String getDcName() {
        return dcName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "DelayMonitorCommandPacket{" +
                "dcName='" + dcName + '\'' +
                ", region='" + region + '\'' +
                ", clusterName='" + clusterName + '\'' +
                "} " + super.toString();
    }
}
