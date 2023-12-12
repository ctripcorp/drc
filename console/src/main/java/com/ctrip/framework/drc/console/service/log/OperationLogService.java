package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.param.log.OperationLogQueryParam;
import com.ctrip.framework.drc.console.vo.log.OperationLogView;
import java.sql.SQLException;
import java.util.List;

public interface OperationLogService {

    List<OperationLogView> getOperationLogView(OperationLogQueryParam param) throws SQLException;
    
    Long gerOperationLogCount(OperationLogQueryParam param) throws SQLException;
}
