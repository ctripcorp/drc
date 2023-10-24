package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.param.log.ConflictRowsLogQueryParam;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.framework.drc.console.vo.log.ConflictCurrentRecordView;
import com.ctrip.framework.drc.console.vo.log.ConflictRowsLogView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogDetailView;
import com.ctrip.framework.drc.console.vo.log.ConflictTrxLogView;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/26 15:09
 */
public interface ConflictLogService {

    List<ConflictTrxLogView> getConflictTrxLogView(ConflictTrxLogQueryParam param) throws Exception;

    List<ConflictRowsLogView> getConflictRowsLogView(ConflictRowsLogQueryParam param) throws Exception;

    ConflictTrxLogDetailView getConflictTrxLogDetailView(Long conflictTrxLogId) throws Exception;

    ConflictCurrentRecordView getConflictCurrentRecordView(Long conflictTrxLogId, int columnSize) throws Exception;

    ConflictCurrentRecordView getConflictRowRecordView(Long conflictRowLogId, int columnSize) throws Exception;

    void createConflictLog(List<ConflictTransactionLog> trxLogs) throws Exception;

    long deleteTrxLogs(long beginTime, long endTime) throws Exception;
}
