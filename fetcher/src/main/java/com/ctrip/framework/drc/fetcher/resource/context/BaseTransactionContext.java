package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.schema.data.Bitmap;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;

import java.util.List;

/**
 * @Author limingdong
 * @create 2021/3/5
 */
public interface BaseTransactionContext extends TimeContext, TableKeyContext, TableKeyMapContext, SequenceNumberContext, GtidContext, DcTagContext, TimeTraceContext {

    void setLastUnbearable(Throwable throwable);

    void setTableKey(TableKey tableKey);

    void insert(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns);

    void update(List<List<Object>> beforeRows, Bitmap beforeBitmap,
                List<List<Object>> afterRows, Bitmap afterBitmap, Columns columns);

    void delete(List<List<Object>> beforeRows, Bitmap beforeBitmap, Columns columns);

    default void setLogEventHeader(LogEventHeader logEventHeader) {

    };

    default TransactionData.ApplyResult complete() {
        return TransactionData.ApplyResult.SUCCESS;
    }
}
