package com.ctrip.framework.drc.core.driver.command.packet.server;

import com.ctrip.framework.drc.core.driver.command.packet.HeaderPacket;
import com.ctrip.framework.drc.core.driver.util.LengthCodedStringReader;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/10 下午3:12.
 */
public class RowDataPacket extends HeaderPacket {

    private List<String> columns = new ArrayList<String>();

    @Override
    public RowDataPacket read(ByteBuf byteBuf) {
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
        LengthCodedStringReader reader = new LengthCodedStringReader(null, index);
        do {
            getColumns().add(reader.readLengthCodedString(data));
        } while (reader.getIndex() < data.length);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String toString() {
        return "RowDataPacket [columns=" + columns + "]";
    }
}
