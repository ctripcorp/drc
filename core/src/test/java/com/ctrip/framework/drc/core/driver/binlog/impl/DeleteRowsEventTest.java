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
public class DeleteRowsEventTest extends AbstractEventTest {

    @Test
    public void testFilterRow() throws IOException {
        int FILTER_IS_NUM = 21;
        DeleteRowsEvent localDeleteRowsEvent = getDeleteRowsEvent();
        List<AbstractRowsEvent.Row> before = localDeleteRowsEvent.getRows();
        List<AbstractRowsEvent.Row> filtered = Lists.newArrayList();
        for (AbstractRowsEvent.Row row : before) {
            int id = (int) row.getBeforeValues().get(0);
            if (id == FILTER_IS_NUM) {  // （20、21、22、23）filter one row
                filtered.add(row);
            }
        }
        localDeleteRowsEvent.setRows(filtered);

        DeleteRowsEvent newDeleteRowsEvent = new FilteredDeleteRowsEvent(localDeleteRowsEvent, columns);

        ByteBuf header = newDeleteRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex();
        ByteBuf payload = newDeleteRowsEvent.getPayloadBuf().resetReaderIndex();
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, header, payload);
        DeleteRowsEvent readFromByteBuf = new DeleteRowsEvent().read(compositeByteBuf);
        readFromByteBuf.load(columns);

        // header
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getFlags(), deleteRowsEvent.getLogEventHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getServerId(), deleteRowsEvent.getLogEventHeader().getServerId());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getEventType(), deleteRowsEvent.getLogEventHeader().getEventType());

        // post header
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getTableId(), deleteRowsEvent.getRowsEventPostHeader().getTableId());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getFlags(), deleteRowsEvent.getRowsEventPostHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraDataLength(), deleteRowsEvent.getRowsEventPostHeader().getExtraDataLength());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraData(), deleteRowsEvent.getRowsEventPostHeader().getExtraData());

        // payload
        Assert.assertEquals(readFromByteBuf.getNumberOfColumns(), deleteRowsEvent.getNumberOfColumns());
        Assert.assertEquals(readFromByteBuf.getBeforePresentBitMap(), deleteRowsEvent.getBeforePresentBitMap());
        Assert.assertEquals(readFromByteBuf.getAfterPresentBitMap(), deleteRowsEvent.getAfterPresentBitMap());
        Assert.assertEquals(readFromByteBuf.getChecksum(), deleteRowsEvent.getChecksum());

        // rows
        List<AbstractRowsEvent.Row> beforeRows = deleteRowsEvent.getRows();
        List<AbstractRowsEvent.Row> afterRows = readFromByteBuf.getRows();
        Assert.assertNotEquals(beforeRows, afterRows);
        Assert.assertEquals(beforeRows.size(), 4);
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


    @Test
    public void testFilterColumn() throws IOException {
        DeleteRowsEvent localDeleteRowsEvent = getDeleteRowsEvent();

        List<Integer> columnsIndex = Lists.newArrayList(0, 2, 3, 4, 5);
        localDeleteRowsEvent.extractColumns(columnsIndex);

        columns.removeColumn("one");
        DeleteRowsEvent newDeleteRowsEvent = new FilteredDeleteRowsEvent(localDeleteRowsEvent, columns);

        ByteBuf header = newDeleteRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex();
        ByteBuf payload = newDeleteRowsEvent.getPayloadBuf().resetReaderIndex();
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer();
        compositeByteBuf.addComponents(true, header, payload);
        DeleteRowsEvent readFromByteBuf = new DeleteRowsEvent().read(compositeByteBuf);
        readFromByteBuf.load(columns);


        // header
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getFlags(), deleteRowsEvent.getLogEventHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getServerId(), deleteRowsEvent.getLogEventHeader().getServerId());
        Assert.assertEquals(readFromByteBuf.getLogEventHeader().getEventType(), deleteRowsEvent.getLogEventHeader().getEventType());

        // post header
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getTableId(), deleteRowsEvent.getRowsEventPostHeader().getTableId());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getFlags(), deleteRowsEvent.getRowsEventPostHeader().getFlags());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraDataLength(), deleteRowsEvent.getRowsEventPostHeader().getExtraDataLength());
        Assert.assertEquals(readFromByteBuf.getRowsEventPostHeader().getExtraData(), deleteRowsEvent.getRowsEventPostHeader().getExtraData());

        // payload
        Assert.assertEquals(readFromByteBuf.getNumberOfColumns(), deleteRowsEvent.getNumberOfColumns() - 1);
        Assert.assertEquals(readFromByteBuf.getBeforePresentBitMap(), deleteRowsEvent.getBeforePresentBitMap());
        Assert.assertEquals(readFromByteBuf.getAfterPresentBitMap(), deleteRowsEvent.getAfterPresentBitMap());
        Assert.assertEquals(readFromByteBuf.getChecksum(), deleteRowsEvent.getChecksum());

        // rows
        List<AbstractRowsEvent.Row> beforeRows = deleteRowsEvent.getRows();
        List<AbstractRowsEvent.Row> afterRows = readFromByteBuf.getRows();
        Assert.assertNotEquals(beforeRows, afterRows);
        Assert.assertEquals(beforeRows.size(), 4);
        Assert.assertEquals(afterRows.size(), 4);

        // columns
        AbstractRowsEvent.Row beforeRow = beforeRows.get(0);
        AbstractRowsEvent.Row afterRow = afterRows.get(0);
        List<Object> before_beforeValue = beforeRow.getBeforeValues();
        List<Object> after_beforeValue = afterRow.getBeforeValues();
        Assert.assertEquals(before_beforeValue.size() - 1, after_beforeValue.size());
        Assert.assertEquals(before_beforeValue.get(0), after_beforeValue.get(0));
        Assert.assertEquals(before_beforeValue.get(2), after_beforeValue.get(1));
        Assert.assertEquals(before_beforeValue.get(3), after_beforeValue.get(2));
        Assert.assertEquals(before_beforeValue.get(4), after_beforeValue.get(3));
        Assert.assertEquals(before_beforeValue.get(5), after_beforeValue.get(4));

        List<Object> before_afterValue = beforeRow.getAfterValues();
        List<Object> after_afterValue = afterRow.getAfterValues();
        Assert.assertNull(before_afterValue);
        Assert.assertNull(after_afterValue);
    }

    @Override
    protected RowsFilterType getRowsFilterType() {
        return RowsFilterType.JavaRegex;
    }

    @Test
    public void testDeleteRowsEventConstruction() throws IOException {
        String oldPayloadBuf = ByteBufUtil.hexDump(deleteRowsEvent.getPayloadBuf().resetReaderIndex());
        String oldHeaderBuf = ByteBufUtil.hexDump(deleteRowsEvent.getLogEventHeader().getHeaderBuf().resetReaderIndex());

        DeleteRowsEvent newDeleteRowsEvent1 = new FilteredDeleteRowsEvent(deleteRowsEvent, columns);
        String newPayloadBuf = ByteBufUtil.hexDump(newDeleteRowsEvent1.getPayloadBuf().resetReaderIndex());
        String newHeaderBuf = ByteBufUtil.hexDump(newDeleteRowsEvent1.getLogEventHeader().getHeaderBuf().resetReaderIndex());
        Assert.assertEquals(oldPayloadBuf, newPayloadBuf);
        Assert.assertNotEquals(oldHeaderBuf, newHeaderBuf); // diff eventTimestamp
    }
}
