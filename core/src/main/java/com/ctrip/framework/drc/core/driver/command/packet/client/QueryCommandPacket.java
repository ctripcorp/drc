package com.ctrip.framework.drc.core.driver.command.packet.client;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.command.AbstractServerCommandWithHeadPacket;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.ctrip.framework.drc.core.driver.command.SERVER_COMMAND.COM_QUERY;

/**
 * Created by mingdongli
 * 2019/9/8 下午5:28.
 */
public class QueryCommandPacket extends AbstractServerCommandWithHeadPacket<QueryCommandPacket> {

    private String queryString;

    public QueryCommandPacket() {
        super(COM_QUERY.getCode());
    }

    public QueryCommandPacket(String queryString) {
        super(COM_QUERY.getCode());
        this.queryString = queryString;
    }

    @Override
    public QueryCommandPacket read(ByteBuf byteBuf) {
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
        try {
            byte[] body = queryString.getBytes("UTF-8");
            out.write(getCommand());
            out.write(body);// 链接建立时默认指定编码为UTF-8
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. command
        setCommand(data[index]);
        index++;
        // 2. read flags
        byte[] queryStringByte = ByteHelper.readFixedLengthBytes(data, index, data.length - 1);
        queryString = new String(queryStringByte);
    }
}
