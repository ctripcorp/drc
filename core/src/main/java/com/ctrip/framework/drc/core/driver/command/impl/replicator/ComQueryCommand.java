package com.ctrip.framework.drc.core.driver.command.impl.replicator;

import com.ctrip.framework.drc.core.driver.command.packet.client.QueryCommandPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.EOFPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.FieldPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.ResultSetPacket;
import com.ctrip.framework.drc.core.driver.command.packet.server.RowDataPacket;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by mingdongli
 * 2019/9/10 上午10:43.
 */
public class ComQueryCommand extends AbstractQueryCommand<ResultSetPacket> {

    private ResultSetStatus resultSetStatus = ResultSetStatus.ToReadHead;

    private ResultSetPacket resultPacket = new ResultSetPacket();

    private List<RowDataPacket> rowData = new ArrayList<RowDataPacket>();

    public ComQueryCommand(QueryCommandPacket queryCommandPacket, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(queryCommandPacket, clientPool, scheduled);
    }

    /**
     * (Result Set Header Packet) the number of columns <br>
     * (Field Packets) column descriptors <br>
     * (EOF Packet) marker: end of Field Packets <br>
     * (Row Data Packets) row contents <br>
     * (EOF Packet) marker: end of Data Packets
     *
     * @param
     * @return
     * @throws IOException
     */
    @Override
    protected ResultSetPacket doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

        switch (resultSetStatus) {
            case ToReadHead:
                resultPacket.read(byteBuf);
                resultSetStatus = ResultSetStatus.ToReadColumn;
                return null;
            case ToReadColumn:
                FieldPacket fieldPacket = new FieldPacket();
                fieldPacket.read(byteBuf);
                resultPacket.getFieldDescriptors().add(fieldPacket);
                if (resultPacket.getColumnCount() == resultPacket.getFieldDescriptors().size()) {
                    resultSetStatus = ResultSetStatus.ToReadEof;
                }
                return null;
            case ToReadEof:
                EOFPacket eofPacket = new EOFPacket();
                eofPacket.read(byteBuf);
                resultSetStatus = ResultSetStatus.ToReadRow;
                return null;
            case ToReadRow:
                if (byteBuf.getByte(MySQLConstants.HEADER_PACKET_LENGTH) == -2) {
                    for (RowDataPacket r : rowData) {
                        resultPacket.getFieldValues().addAll(r.getColumns());
                    }
                    resultPacket.setSourceAddress(channel.remoteAddress());

                    List<String> columnValues = resultPacket.getFieldValues();
                    if (columnValues != null && columnValues.size() >= 1 && columnValues.get(0).toUpperCase().equals("CRC32")) {
                        getLogger().info("binlogChecksum is BINLOG_CHECKSUM_ALG_CRC32");
                    } else {
                        getLogger().info("binlogChecksum is BINLOG_CHECKSUM_ALG_OFF");
                    }

                    resultSetStatus = ResultSetStatus.LastEof;
                    return null;
                }
                RowDataPacket rowDataPacket = new RowDataPacket();
                rowDataPacket.read(byteBuf);
                rowData.add(rowDataPacket);
                return null;
            case LastEof:
                EOFPacket lastEofPacket = new EOFPacket();
                lastEofPacket.read(byteBuf);
                return resultPacket;
            default:
                return null;
        }
    }

    enum ResultSetStatus {

        ToReadHead,

        ToReadColumn,

        ToReadRow,

        ToReadEof,

        LastEof
    }
}
