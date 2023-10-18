package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTable;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.framework.drc.fetcher.conflict.enums.ConflictResult;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * @ClassName TransactionInfoRecorder
 * @Author haodongPan
 * @Date 2023/10/17 17:51
 * @Version: $
 */
public class TransactionLogRecorder {

    private  int recordSize = 100;
    private ConflictTransactionLog cflTrxLog;
    private long trxRowNum;
    private long conflictRowNum;
    private long rollbackRowNum;
    private PriorityQueue<ConflictRowLog> cflRowLogsQueue;
    private Map<ConflictTable,Long> conflictTableRowsCount;
    
    public TransactionLogRecorder(int recordSize) {
        this.recordSize = recordSize;
        this.cflTrxLog = new ConflictTransactionLog();
        this.trxRowNum = 0L;
        this.conflictRowNum = 0L;
        this.rollbackRowNum = 0L;
        this.cflRowLogsQueue = new PriorityQueue<>(recordSize);
        this.conflictTableRowsCount = Maps.newHashMap();
    }
    
    public ConflictTransactionLog summaryBeforeReport(String gtid) {
        List<ConflictRowLog> cflLogs = new ArrayList<>(cflRowLogsQueue.size());
        while (cflRowLogsQueue.size() > 0) {
            cflLogs.add(0,cflRowLogsQueue.poll());
        }
        cflTrxLog.setCflLogs(cflLogs);
        cflTrxLog.setTrxRes(rollbackRowNum == 0 ? ConflictResult.COMMIT.getValue() : ConflictResult.ROLLBACK.getValue());
        cflTrxLog.setGtid(gtid);
        cflTrxLog.setTrxRowsNum(trxRowNum);
        cflTrxLog.setCflRowsNum(conflictRowNum);
        return cflTrxLog;
    }
    
    // rowsRes: commit out first, then rowsId: bigger one out first
    public boolean recordCflRowLogIfNecessary(ConflictRowLog curCflRowLog) {
        cflTableCount(curCflRowLog);
        conflictRowNum++;
        if (ConflictResult.ROLLBACK.getValue() == curCflRowLog.getRowRes()) {
            rollbackRowNum++;
            if (rollbackRowNum > recordSize) {
                return false;
            }
            cflRowLogsQueue.add(curCflRowLog);
            if (cflRowLogsQueue.size() > recordSize) {
                cflRowLogsQueue.poll();
            }
            return true;
        } else {
            if (cflRowLogsQueue.size() >= recordSize) {
                return false;
            }
            cflRowLogsQueue.add(curCflRowLog);
            return true;
        }
    }
    

    public void trxRowNumIncrement() {
        trxRowNum++;
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public ConflictTransactionLog getCflTrxLog() {
        return cflTrxLog;
    }

    public void setCflTrxLog(ConflictTransactionLog cflTrxLog) {
        this.cflTrxLog = cflTrxLog;
    }

    public long getTrxRowNum() {
        return trxRowNum;
    }

    public void setTrxRowNum(long trxRowNum) {
        this.trxRowNum = trxRowNum;
    }

    public long getConflictRowNum() {
        return conflictRowNum;
    }

    public void setConflictRowNum(long conflictRowNum) {
        this.conflictRowNum = conflictRowNum;
    }

    public long getRollbackRowNum() {
        return rollbackRowNum;
    }

    public void setRollbackRowNum(long rollbackRowNum) {
        this.rollbackRowNum = rollbackRowNum;
    }

    public PriorityQueue<ConflictRowLog> getCflRowLogsQueue() {
        return cflRowLogsQueue;
    }

    public void setCflRowLogsQueue(
            PriorityQueue<ConflictRowLog> cflRowLogsQueue) {
        this.cflRowLogsQueue = cflRowLogsQueue;
    }

    public Map<ConflictTable, Long> getConflictTableRowsCount() {
        return conflictTableRowsCount;
    }

    public void setConflictTableRowsCount(
            Map<ConflictTable, Long> conflictTableRowsCount) {
        this.conflictTableRowsCount = conflictTableRowsCount;
    }

    
    private void cflTableCount(ConflictRowLog curCflRowLog) {
        // for hickWall report
        ConflictTable thisRow =  new ConflictTable(curCflRowLog.getDb(),curCflRowLog.getTable(), curCflRowLog.getRowRes());
        Long count = conflictTableRowsCount.getOrDefault(thisRow, 0L);
        conflictTableRowsCount.put(thisRow,++count);
    }
}
