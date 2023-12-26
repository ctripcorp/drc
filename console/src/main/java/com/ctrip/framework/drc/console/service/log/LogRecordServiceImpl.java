package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.log.OperationLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;
import com.ctrip.platform.dal.dao.DalHints;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName LogRecordServiceImpl
 * @Author haodongPan
 * @Date 2023/12/6 15:37
 * @Version: $
 */
@Service
public class LogRecordServiceImpl implements LogRecordService{
    
    @Autowired
    private OperationLogTblDao operationLogDao;
    
    private final Logger opLogger = LoggerFactory.getLogger("OPERATION");
    
    
    @Override
    public void record(OperationLogTbl operationLog,boolean persisted) {
        opLogger.info("operationLog: [{}]", operationLog);
        if (persisted) {
            try {
                operationLogDao.insert(new DalHints().asyncExecution(),operationLog);
            } catch (SQLException e) {
                opLogger.error("persist fail,operationLog: [{}]", operationLog,e);
            }
        }
    }
}
