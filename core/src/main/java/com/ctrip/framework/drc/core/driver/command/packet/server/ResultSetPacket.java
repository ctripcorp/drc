package com.ctrip.framework.drc.core.driver.command.packet.server;

import io.netty.buffer.ByteBuf;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/10 上午10:27.
 */
public class ResultSetPacket extends ResultSetHeaderPacket {

    private SocketAddress sourceAddress;

    private List<FieldPacket> fieldDescriptors = new ArrayList<FieldPacket>();

    private List<String>      fieldValues      = new ArrayList<String>();

    @Override
    public ResultSetPacket read(ByteBuf byteBuf) {
        super.read(byteBuf);
        return this;
    }

    public void setFieldDescriptors(List<FieldPacket> fieldDescriptors) {
        this.fieldDescriptors = fieldDescriptors;
    }

    public List<FieldPacket> getFieldDescriptors() {
        return fieldDescriptors;
    }

    public void setFieldValues(List<String> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public List<String> getFieldValues() {
        return fieldValues;
    }

    public void setSourceAddress(SocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public SocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public String toString() {
        return "ResultSetPacket [fieldDescriptors=" + fieldDescriptors + ", fieldValues=" + fieldValues
                + ", sourceAddress=" + sourceAddress + "]";
    }

}
