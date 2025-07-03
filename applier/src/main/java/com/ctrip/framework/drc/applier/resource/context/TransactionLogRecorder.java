package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTable;
import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.monitor.enums.ConflictDetail;
import com.ctrip.framework.drc.core.monitor.enums.ConflictResult;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
    private long conflictNeedRocordRowNum;
    private PriorityQueue<ConflictRowLog> cflRowLogsQueue;
    private Map<ConflictTable, CflCountDetail> conflictTableRowsCount;
    private final String CACHE_KEY = "uploadLevel";

    private final LoadingCache<String, Set<ConflictDetail.AlertLevel>> uploadLevelCache = CacheBuilder.newBuilder()
            .maximumSize(1)
            .expireAfterAccess(60, TimeUnit.SECONDS)
            .build(new CacheLoader<>() {
                @Override
                public Set<ConflictDetail.AlertLevel> load(String key) {
                    return ApplierDynamicConfig.getInstance().getConflictLogUpLevel();
                }
            });
    
    public TransactionLogRecorder(int recordSize) {
        this.recordSize = recordSize;
        this.cflTrxLog = new ConflictTransactionLog();
        this.trxRowNum = 0L;
        this.conflictRowNum = 0L;
        this.rollbackRowNum = 0L;
        this.conflictNeedRocordRowNum = 0L;
        this.cflRowLogsQueue = new PriorityQueue<>(recordSize);
        this.conflictTableRowsCount = Maps.newHashMap();
    }

    /**
     * @return null if no need to report
     */
    public ConflictTransactionLog summaryBeforeReport(String gtid) {
        List<ConflictRowLog> cflLogs = new ArrayList<>(cflRowLogsQueue.size());
        //record situation that other normal rows affected by roll back rows
        boolean needTotalReport = (rollbackRowNum != 0 && trxRowNum != rollbackRowNum);
        while (cflRowLogsQueue.size() > 0) {
            ConflictRowLog rowLog = cflRowLogsQueue.poll();
            if (needTotalReport || rowLog.getNeedRecord() == 1) {
                cflLogs.add(0, rowLog);
            }
        }
        if (cflLogs.isEmpty()) {
            return null;
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
        doNeedRecord(curCflRowLog);
        conflictRowNum++;
        if (ConflictResult.ROLLBACK.getValue() == curCflRowLog.getRowRes()) {
            rollbackRowNum++;
            if (curCflRowLog.getNeedRecord() == 1) {
                conflictNeedRocordRowNum++;
            }
            if (conflictNeedRocordRowNum > recordSize) {
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

    private boolean doNeedRecord(ConflictRowLog curCflRowLog) {
        try {
            Set<ConflictDetail.AlertLevel> uploadLevel = uploadLevelCache.get(CACHE_KEY);
            boolean needRecord = uploadLevel.contains(curCflRowLog.getConflictDetailEnum().getAlertLevel());
            curCflRowLog.setNeedRecord(needRecord? 1:0);
            return needRecord;
        } catch (ExecutionException ignored) {
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

    public Map<ConflictTable, CflCountDetail> getConflictTableRowsCount() {
        return conflictTableRowsCount;
    }

    public void setConflictTableRowsCount(
            Map<ConflictTable, CflCountDetail> conflictTableRowsCount) {
        this.conflictTableRowsCount = conflictTableRowsCount;
    }


    private void cflTableCount(ConflictRowLog curCflRowLog) {
        // for hickWall report
        ConflictTable thisRow =  new ConflictTable(curCflRowLog.getDb(),curCflRowLog.getTable(), curCflRowLog.getRowRes());
        CflCountDetail cflCountDetail = conflictTableRowsCount.computeIfAbsent(thisRow, key -> new CflCountDetail());
        cflCountDetail.add(curCflRowLog.getConflictDetail());
    }

    public static class CflCountDetail {
        private Long cnt;
        private Map<String,Long> conflictDetailCount;

        public CflCountDetail() {
            cnt = 0L;
            conflictDetailCount = Maps.newHashMap();
        }

        public void add(String conflictDetail) {
            cnt++;
            Long detailCount = conflictDetailCount.getOrDefault(conflictDetail, 0L);
            conflictDetailCount.put(conflictDetail, ++detailCount);
        }

        public Long getCnt() {
            return cnt;
        }

        public Map<String, Long> getConflictDetailCount() {
            return conflictDetailCount;
        }
    }
}
