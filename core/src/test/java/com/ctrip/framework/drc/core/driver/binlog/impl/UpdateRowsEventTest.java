package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.AbstractEventTest;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 */
public class UpdateRowsEventTest extends AbstractEventTest {

    @Test
    public void testFilterRow() throws IOException {
        int FILTER_IS_NUM = 12;
        UpdateRowsEvent localUpdateRowsEvent = getUpdateRowsEvent();
        List<AbstractRowsEvent.Row> before = localUpdateRowsEvent.getRows();
        List<AbstractRowsEvent.Row> filtered = Lists.newArrayList();
        for (AbstractRowsEvent.Row row : before) {
            int id = (int) row.getAfterValues().get(0);
            if (id == FILTER_IS_NUM) {  // （11、12、13）filter one row
                filtered.add(row);
            }
        }
        localUpdateRowsEvent.setRows(filtered);

        UpdateRowsEvent newUpdateRowsEvent = new FilteredUpdateRowsEvent(localUpdateRowsEvent, columns);

        ByteBuf header = newUpdateRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex();
        ByteBuf payload = newUpdateRowsEvent.getPayloadBuf().resetReaderIndex();
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, header, payload);
        WriteRowsEvent readFromByteBuf = new WriteRowsEvent().read(compositeByteBuf);
        readFromByteBuf.load(columns);

        // header
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getFlags(), updateRowsEvent.getLogEventHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getServerId(), updateRowsEvent.getLogEventHeader().getServerId());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getEventType(), updateRowsEvent.getLogEventHeader().getEventType());

        // post header
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getTableId(), updateRowsEvent.getRowsEventPostHeader().getTableId());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getFlags(), updateRowsEvent.getRowsEventPostHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraDataLength(), updateRowsEvent.getRowsEventPostHeader().getExtraDataLength());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraData(), updateRowsEvent.getRowsEventPostHeader().getExtraData());

        // payload
        Assert.assertEquals(readFromByteBuf.getNumberOfColumns(), updateRowsEvent.getNumberOfColumns());
        Assert.assertEquals(readFromByteBuf.getBeforePresentBitMap(), updateRowsEvent.getBeforePresentBitMap());
        Assert.assertEquals(readFromByteBuf.getAfterPresentBitMap(), updateRowsEvent.getAfterPresentBitMap());
        Assert.assertEquals(readFromByteBuf.getChecksum(), updateRowsEvent.getChecksum());

        // rows
        List<AbstractRowsEvent.Row> beforeRows = updateRowsEvent.getRows();
        List<AbstractRowsEvent.Row> afterRows = readFromByteBuf.getRows();
        Assert.assertNotEquals(beforeRows, afterRows);
        Assert.assertEquals(beforeRows.size(), 3);
        Assert.assertEquals(afterRows.size(), 1);

        List<AbstractRowsEvent.Row> filteredBeforeRows = Lists.newArrayList();
        for (AbstractRowsEvent.Row row : beforeRows) {
            int id = (int) row.getBeforeValues().get(0);
            if (id == FILTER_IS_NUM) {
                filteredBeforeRows.add(row);
            }
        }
        Assert.assertEquals(filteredBeforeRows.get(0).getBeforeValues(), afterRows.get(0).getBeforeValues());
        Assert.assertEquals(filteredBeforeRows.get(0).getAfterValues(), afterRows.get(0).getAfterValues());
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.JavaRegex;
    }

    @Test
    public void testUpdateRowsEventConstruction() throws IOException {
        String oldPayloadBuf = ByteBufUtil.hexDump(updateRowsEvent.getPayloadBuf().resetReaderIndex());
        String oldHeaderBuf = ByteBufUtil.hexDump(updateRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex());

        UpdateRowsEvent newUpdateRowsEvent1 = new FilteredUpdateRowsEvent(updateRowsEvent, columns);
        String newPayloadBuf = ByteBufUtil.hexDump(newUpdateRowsEvent1.getPayloadBuf().resetReaderIndex());
        String newHeaderBuf = ByteBufUtil.hexDump(newUpdateRowsEvent1.getLogEventHeader().getHeaderBuf().resetReaderIndex());
        Assert.assertEquals(oldPayloadBuf, newPayloadBuf);
        Assert.assertNotEquals(oldHeaderBuf, newHeaderBuf); // diff eventTimestamp
    }
}
