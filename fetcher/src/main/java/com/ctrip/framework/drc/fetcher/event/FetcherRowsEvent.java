package com.ctrip.framework.drc.fetcher.event;

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.event.meta.MetaEvent;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionEvent;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemory;
import com.ctrip.framework.drc.fetcher.resource.condition.DirectMemoryAware;
import com.ctrip.framework.drc.fetcher.resource.context.BaseTransactionContext;
import com.ctrip.framework.drc.fetcher.resource.context.LinkContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
        TableKey targetTable = linkContext.fetchTableKey();
        columns = linkContext.fetchColumns(targetTable);
        originColumns = linkContext.fetchColumns();
        if (columns == null) {
            throw new Exception("columns not found, TableKey: " + targetTable.toString() + " - UNLIKELY");
        }
        transformMetaAndType();
    }

    private Lock lock = new ReentrantLock();
    private boolean isLoaded = false;

    public void tryLoad() {
        if (lock.tryLock()) {
            try {
                if (!isLoaded) {
                    load(columns);
                    isLoaded = true;
                }
            } catch (Throwable t) {
                logger.error(getClass() + ".tryLoad() - UNLIKELY", t);
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
            logger.error(getClass() + ".mustLoad() - UNLIKELY", t);
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

    /**
     * transform outside load() for multi rows event
     */
    private void transformMetaAndType() {
        for (int i = 0; i < originColumns.size(); ++i) {
            MysqlFieldType type = MysqlFieldType.getMysqlFieldType(originColumns.get(i).getType());
            if (MysqlFieldType.mysql_type_string.equals(type)) {
                int meta = originColumns.get(i).getMeta();
                if (meta >= 256) {
                    final Pair<Integer, Integer> realMetaAndType = getRealMetaAndType(meta);
                    // meta
                    columns.get(i).setMeta(realMetaAndType.getKey());

                    // type
                    int typeInt = realMetaAndType.getValue();
                    if (!type.equals(MysqlFieldType.mysql_type_set)
                            && !type.equals(MysqlFieldType.mysql_type_enum)
                            && !type.equals(MysqlFieldType.mysql_type_string)) {
                        throw new IllegalStateException("MySQL binlog string type can only be converted into string, enum, set types.");
                    }
                    columns.get(i).setType(typeInt);
                }
            } else {
                columns.get(i).setMeta(originColumns.get(i).getMeta());  //update meta
                columns.get(i).setType(originColumns.get(i).getType());  //update type
            }
        }
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
