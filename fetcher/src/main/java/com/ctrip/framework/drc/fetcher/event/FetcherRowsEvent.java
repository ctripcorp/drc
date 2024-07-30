package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ctrip.framework.drc.core.server.utils.RowsEventUtils.transformMetaAndType;

/**
 * @Author Slight
 * Oct 17, 2019
 */
public abstract class FetcherRowsEvent<T extends BaseTransactionContext> extends AbstractRowsEvent implements TransactionEvent<T>, MetaEvent.Read<LinkContext>, DirectMemoryAware {

    protected static final Logger logger = LoggerFactory.getLogger(FetcherRowsEvent.class);
    protected static final Logger loggerR = LoggerFactory.getLogger("ROWS");
    private static final int END_OF_STATEMENT_FLAG = 1;

    protected Columns columns;  //drc_table_map_log_event
    protected Columns originColumns;  //just having meta and nullable
    protected String gtid;
    protected int dataIndex;
    private DirectMemory directMemory;

    @Override
    public void involve(LinkContext linkContext) throws Exception {
        gtid = linkContext.fetchGtid();
        dataIndex = linkContext.increaseDataIndexByOne();
        loadPostHeaderLocked();
        TableKey targetTable = linkContext.fetchTableKeyInMap(getRowsEventPostHeader().getTableId());
        if (getRowsEventPostHeader().getFlags() == END_OF_STATEMENT_FLAG) {
            linkContext.resetTableKeyMap();
        }
        columns = linkContext.fetchColumns(targetTable);
        originColumns = Columns.from(targetTable.getColumns());
        if (columns == null) {
            throw new Exception("columns not found, TableKey: " + targetTable.toString() + " - UNLIKELY");
        }
        transformMetaAndType(originColumns, columns);
    }

    @Override
    public boolean shouldDecodeBinaryJson() {
        return true;
    }

    private Lock lock = new ReentrantLock();
    private volatile boolean isLoaded = false;

    private void loadPostHeaderLocked() {
        lock.lock();
        try {
            loadPostHeader();
        } catch (Throwable t) {
            logger.error("{}.loadPostHeaderLocked() - UNLIKELY for {}, {}", getClass(), gtid, t);
        } finally {
            lock.unlock();
        }
    }

    public void tryLoad() {
        if (lock.tryLock()) {
            try {
                if (!isLoaded) {
                    load(columns);
                    isLoaded = true;
                }
            } catch (Throwable t) {
                ByteBuf headerByteBuf = getLogEventHeader().getHeaderBuf();
                ByteBuf payloadByteBuf = getPayloadBuf();
                logger.error("{}.tryLoad() - UNLIKELY for {}, {}, {}", getClass(), gtid, ByteBufUtil.hexDump(headerByteBuf, 0, headerByteBuf.writerIndex()), ByteBufUtil.hexDump(payloadByteBuf, 0, headerByteBuf.writerIndex()), t);
            } finally {
                lock.unlock();
            }
        }
    }

    public void mustLoad(T context) {
        lock.lock();
        try {
            if (!isLoaded) {
                context.atTrace("l");
                load(columns);
                context.atTrace("L");
                isLoaded = true;
            }
        } catch (Throwable t) {
            ByteBuf headerByteBuf = getLogEventHeader().getHeaderBuf();
            ByteBuf payloadByteBuf = getPayloadBuf();
            logger.error("{}.mustLoad() - UNLIKELY for {}, {}, {}", getClass(), gtid, ByteBufUtil.hexDump(headerByteBuf, 0, headerByteBuf.writerIndex()), ByteBufUtil.hexDump(payloadByteBuf, 0, headerByteBuf.writerIndex()), t);
        } finally {
            lock.unlock();
        }
    }

    private AtomicBoolean released = new AtomicBoolean(false);

    @Override
    public void release() {
        if (released.compareAndSet(false, true)) {
            directMemory.release(getLogEventHeader().getEventSize());
            super.release();
        }
    }

    public ApplyResult apply(T context) {
        try {
            mustLoad(context);
            release();
            try {
                beforeDoApply(context);
                doApply(context);
                afterDoApply(context);
                return ApplyResult.SUCCESS;
            } catch (Throwable e) {
                logger.error(attachTags(context, getClass() + ".doApply() - UNLIKELY"), e);
                context.setLastUnbearable(e);
                return ApplyResult.LOAD;
            }
        } catch (Throwable e) {
            logger.error(attachTags(context, getClass() + ".load() - UNLIKELY"), e);
            context.setLastUnbearable(e);
            return ApplyResult.UNKNOWN;
        }
    }

    @Override
    public void setDirectMemory(DirectMemory directMemory) {
        this.directMemory = directMemory;
    }

    private void beforeDoApply(T context) {
        TableKey tableKey = context.fetchTableKeyInMap(getRowsEventPostHeader().getTableId());
        context.setTableKey(tableKey);
    }

    private void afterDoApply(T context) {
        int flag = getRowsEventPostHeader().getFlags();
        if (flag == END_OF_STATEMENT_FLAG) {
            context.resetTableKeyMap();
        }
    }

    protected abstract void doApply(T context);

    protected String attachTags(T context, String message) {
        return message +
                "\n - GTID: " + context.fetchGtid() +
                " - sequence number: " + context.fetchSequenceNumber() +
                " - table key: " + context.fetchTableKey().toString() +
                "\n - column names: " + columns.getNames();
    }

    @VisibleForTesting
    public boolean isLoaded() {
        return isLoaded;
    }

    @VisibleForTesting
    public void setColumns(Columns columns) {
        this.columns = columns;
    }

    @VisibleForTesting
    public Columns getColumns() {
        return columns;
    }
}
