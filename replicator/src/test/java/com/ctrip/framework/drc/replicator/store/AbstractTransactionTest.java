package com.ctrip.framework.drc.replicator.store;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.TransactionCache;
import com.ctrip.framework.drc.replicator.store.manager.file.FileManager;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/26 下午8:23.
 */
public abstract class AbstractTransactionTest extends AbstractEventTest {

    protected FileManager fileManager;

    protected FilePersistenceEventStore ioCache;

    protected TransactionCache transactionCache;

    protected static final String PREVIOUS_GTID = "5f9a1806-e024-11e9-8588-fa163e7af2aa:1-3,5f9a1806-e024-11e9-8588-fa163e7af2ac:1-3:5-5:7-8,5f9a1806-e024-11e9-8588-fa163e7af2ad:1-18";

    protected static final String UUID_STRING = "a0a1fbb8-bdc8-11e9-96a0-fa163e7af2ad";

    protected static final String DRC_UUID_STRING = "a0a1fcc9-bdc8-11e9-96a0-fa163e7af2be";

    protected int writeTransaction() throws Exception {
        int res = 0;
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getCharsetTypeTableMapEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getMinimalRowsEventByteBuf();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getXidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        return res;
    }

    protected int writeTransaction(String dbName) throws Exception {
        int res = 0;
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        TableMapLogEvent tableMapLogEvent = getFilteredTableMapLogEvent(dbName, "unitest", 123);
        byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        fileManager.append(byteBuf);
        byteBuf = tableMapLogEvent.getPayloadBuf();
        fileManager.append(byteBuf);

        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getMinimalRowsEventByteBuf();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getXidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        return res;
    }

    protected int writeTransactionWithMultiTableMapLogEvent() throws Exception {
        int res = 0;
        ByteBuf byteBuf = getGtidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();

        //one table map event, should send
        res += writeTableMapEvent(fileManager, "db1", "table1",123);
        byteBuf = getMinimalRowsEventByteBuf();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();

        //two table map events
        res += writeTableMapEvent(fileManager, "db1", "table1",123);
        res += writeTableMapEvent(fileManager, "db1", "table2",124);
        byteBuf = getMinimalRowsEventByteBufWithNotEndOfStatement();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        byteBuf = getMinimalRowsEventByteBuf();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();

        //one table map event, should skip
        res += writeTableMapEvent(fileManager, "db1", "table2",124);
        byteBuf = getMinimalRowsEventByteBufWithEndOfStatement();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();

        byteBuf = getXidEvent();
        fileManager.append(byteBuf);
        res += byteBuf.writerIndex();
        byteBuf.release();
        return res;
    }

    private int writeTableMapEvent(FileManager fileManager, String dbName, String tableName, long tableId) throws IOException {
        TableMapLogEvent tableMapLogEvent = getFilteredTableMapLogEvent(dbName, tableName,tableId);
        ByteBuf byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        fileManager.append(byteBuf);
        byteBuf = tableMapLogEvent.getPayloadBuf();
        fileManager.append(byteBuf);
        int res = byteBuf.writerIndex();
        byteBuf.release();
        return res;
    }

    protected int writeTransactionWithGtid(String gtid) throws Exception {
        int res = 0;
        GtidLogEvent gtidLogEvent = new GtidLogEvent(gtid);
        List<ByteBuf> byteBufs = Lists.newArrayList();
        ByteBuf gtidHeader = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        res += gtidHeader.writerIndex();
        ByteBuf gtidPayload = gtidLogEvent.getPayloadBuf();
        res += gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        ByteBuf byteBuf = getCharsetTypeTableMapEvent();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        byteBuf = getMinimalRowsEventByteBuf();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        byteBuf = getXidEvent();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        fileManager.append(byteBufs, false);
        for (ByteBuf bb : byteBufs) {
            bb.release();
        }

        return res;
    }

    protected int writeTransactionWithGtidAndTransactionOffset(String gtid) throws Exception {
        int res = 0;
        GtidLogEvent gtidLogEvent = new GtidLogEvent(gtid);
        List<ByteBuf> byteBufs = Lists.newArrayList();


        ByteBuf tableMapByteBuf = getCharsetTypeTableMapEvent();
        res += tableMapByteBuf.writerIndex();

        ByteBuf rowsByteBuf = getMinimalRowsEventByteBuf();
        res += rowsByteBuf.writerIndex();

        ByteBuf xidByteBuf = getXidEvent();
        res += xidByteBuf.writerIndex();

        gtidLogEvent.setNextTransactionOffsetAndUpdateEventSize(res);

        ByteBuf gtidHeader = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        gtidHeader.writerIndex();
        ByteBuf gtidPayload = gtidLogEvent.getPayloadBuf();
        gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        byteBufs.add(tableMapByteBuf);
        byteBufs.add(rowsByteBuf);
        byteBufs.add(xidByteBuf);


        fileManager.append(byteBufs, false);
        for (ByteBuf bb : byteBufs) {
            bb.release();
        }

        return res;
    }

    protected int writeDrcGtidLogEvent(String gtid) throws Exception {
        int res = 0;
        GtidLogEvent gtidLogEvent = new GtidLogEvent(gtid);
        gtidLogEvent.setEventType(LogEventType.drc_gtid_log_event.getType());
        List<ByteBuf> byteBufs = Lists.newArrayList();
        ByteBuf gtidHeader = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        res += gtidHeader.writerIndex();
        ByteBuf gtidPayload = gtidLogEvent.getPayloadBuf();
        res += gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        fileManager.append(byteBufs, false);
        for (ByteBuf bb : byteBufs) {
            bb.release();
        }

        return res;
    }

    protected int writeTransactionWithGtidAndDdl(String gtid) throws Exception {
        int res = 0;
        GtidLogEvent gtidLogEvent = new GtidLogEvent(gtid);
        List<ByteBuf> byteBufs = Lists.newArrayList();
        ByteBuf gtidHeader = gtidLogEvent.getLogEventHeader().getHeaderBuf();
        res += gtidHeader.writerIndex();
        ByteBuf gtidPayload = gtidLogEvent.getPayloadBuf();
        res += gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        DrcDdlLogEvent drcDdlLogEvent = getDrcDdlLogEvent();
        gtidHeader = drcDdlLogEvent.getLogEventHeader().getHeaderBuf();
        res += gtidHeader.writerIndex();
        gtidPayload = drcDdlLogEvent.getPayloadBuf();
        res += gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();
        gtidHeader = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        res += gtidHeader.writerIndex();
        gtidPayload = tableMapLogEvent.getPayloadBuf();
        res += gtidPayload.writerIndex();
        byteBufs.add(gtidHeader);
        byteBufs.add(gtidPayload);

        ByteBuf byteBuf = getXidEvent();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        fileManager.append(byteBufs, true);
        for (ByteBuf bb : byteBufs) {
            bb.release();
        }

        return res;
    }

    protected void writeGrandTransaction() throws Exception {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        transactionCache.add(gtidLogEvent);
        byteBuf.release();

        for (int i = 0; i < 5000; i++) {
            byteBuf = getCharsetTypeTableMapEvent();
            TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
            transactionCache.add(tableMapLogEvent);
            byteBuf.release();

            byteBuf = getMinimalRowsEventByteBuf();
            UpdateRowsEvent updateRowsEvent = new UpdateRowsEvent().read(byteBuf);
            transactionCache.add(updateRowsEvent);
            byteBuf.release();
        }

        byteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(byteBuf);
        transactionCache.add(xidLogEvent);
        byteBuf.release();
    }

    protected void writeTransactionThroughTransactionCache() throws Exception {
        ByteBuf byteBuf = getGtidEvent();
        GtidLogEvent gtidLogEvent = new GtidLogEvent().read(byteBuf);
        transactionCache.add(gtidLogEvent);
        byteBuf.release();

        byteBuf = getCharsetTypeTableMapEvent();
        TableMapLogEvent tableMapLogEvent = new TableMapLogEvent().read(byteBuf);
        transactionCache.add(tableMapLogEvent);
        byteBuf.release();

        byteBuf = getMinimalRowsEventByteBuf();
        UpdateRowsEvent updateRowsEvent = new UpdateRowsEvent().read(byteBuf);
        transactionCache.add(updateRowsEvent);
        byteBuf.release();

        byteBuf = getXidEvent();
        XidLogEvent xidLogEvent = new XidLogEvent().read(byteBuf);
        transactionCache.add(xidLogEvent);
        byteBuf.release();
    }

    protected int writeDdlTransaction() throws Exception {
        int res = 0;
        List<ByteBuf> byteBufs = Lists.newArrayList();
        ByteBuf byteBuf = getGtidEvent();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        DrcDdlLogEvent drcDdlLogEvent = getDrcDdlLogEvent();
        byteBuf = drcDdlLogEvent.getLogEventHeader().getHeaderBuf();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        byteBuf = drcDdlLogEvent.getPayloadBuf();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        TableMapLogEvent tableMapLogEvent = getDrcTableMapLogEvent();
        byteBuf = tableMapLogEvent.getLogEventHeader().getHeaderBuf();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        byteBuf = tableMapLogEvent.getPayloadBuf();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        byteBuf = getXidEvent();
        byteBufs.add(byteBuf);
        res += byteBuf.writerIndex();

        fileManager.append(byteBufs, true);

        for (ByteBuf byteBuf1 : byteBufs) {
            byteBuf1.release();
        }

        return res;
    }

    protected void writePreviousGtid() throws Exception {
        ByteBuf byteBuf = getPreviousGtidEvent();
        fileManager.append(byteBuf);
        byteBuf.release();
    }
}
