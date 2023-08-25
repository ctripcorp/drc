package com.ctrip.framework.drc.console.service.v2.dbmigration;

import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;

import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.core.http.PageResult;

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
    
    boolean exStartDbMigrationTask(Long taskId) throws SQLException;

    boolean startDbMigrationTask(Long taskId) throws SQLException;

    String getAndUpdateTaskStatus(Long taskId);

    PageResult<MigrationTaskTbl> queryByPage(MigrationTaskQuery query);

    void offlineOldDrcConfig(long taskId) throws Exception;

    void rollBackNewDrcConfig(long taskId) throws Exception;

}
