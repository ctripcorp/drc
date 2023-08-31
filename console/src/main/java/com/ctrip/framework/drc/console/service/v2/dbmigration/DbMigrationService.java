package com.ctrip.framework.drc.console.service.v2.dbmigration;

import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.core.http.PageResult;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @ClassName DbMigrationService
 * @Author haodongPan
 * @Date 2023/8/14 11:48
 * @Version: $
 */
public interface DbMigrationService {

    boolean abandonTask(Long taskId) throws SQLException;

    // return null when no dbDrcRelated
    // return taskId when task create; 
    // throw ConsoleException with reason when forbidden
    Pair<String,Long> dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException;
    
    boolean preStartDbMigrationTask(Long taskId) throws SQLException;

    boolean startDbMigrationTask(Long taskId) throws SQLException;

    String getAndUpdateTaskStatus(Long taskId);

    PageResult<MigrationTaskTbl> queryByPage(MigrationTaskQuery query);

    void offlineOldDrcConfig(long taskId) throws Exception;

    void rollBackNewDrcConfig(long taskId) throws Exception;

}
