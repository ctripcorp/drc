package com.ctrip.framework.drc.console.service.v2.dbmigration;

import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.core.http.ApiResult;
import java.sql.SQLException;

/**
 * @ClassName DbMigrationService
 * @Author haodongPan
 * @Date 2023/8/14 11:48
 * @Version: $
 */
public interface DbMigrationService {
    
    // return null when no dbDrcRelated
    // return taskId when task create; 
    // throw ConsoleException with reason when forbidden
    Long dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException;
    
    boolean startDbMigrationTask(Long taskId) throws SQLException;
    
    
    
}
