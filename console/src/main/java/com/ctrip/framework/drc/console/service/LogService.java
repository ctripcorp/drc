package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.ConflictTransactionLog;
import com.ctrip.framework.drc.console.dto.LogDto;
import com.ctrip.framework.drc.console.dto.LogHandleDto;
import com.ctrip.platform.dal.dao.DalHints;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2020/6/22
 */
public interface LogService {

    void uploadConflictLog(List<ConflictTransactionLog> conflictTransactionLogList);

    void uploadSampleLog(LogDto logDto);

    void deleteLog(DalHints hints);

    Map<String, Object> getLogs(int pageNo, int pageSize);

    Map<String, Object> getLogs(int pageNo, int pageSize, String keyWord);

    Map<String, Object> getConflictLog(int pageNo, int pageSize, String keyWord);

    Map<String, Object> getCurrentRecord(long primaryKey) throws Exception;

    void updateRecord(Map<String, String> updateInfo) throws Exception;
}
