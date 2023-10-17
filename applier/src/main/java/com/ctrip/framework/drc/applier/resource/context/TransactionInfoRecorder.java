package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTable;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * @ClassName TransactionInfoRecorder
 * @Author haodongPan
 * @Date 2023/10/17 17:51
 * @Version: $
 */
public class TransactionInfoRecorder {
    
    private ConflictTransactionLog cflTrxLog;
    private MutableLong trxRowNum;
    private MutableLong conflictRowNum;
    private MutableLong rollbackRowNum;
    private PriorityQueue<ConflictRowLog> cflRowLogsQueue;
    private Map<ConflictTable,Long> conflictTableRowsCount;

}
