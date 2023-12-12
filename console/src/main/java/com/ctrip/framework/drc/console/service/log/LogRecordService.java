package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;

public interface LogRecordService {
    void record(OperationLogTbl operationLog,boolean persisted);
}
