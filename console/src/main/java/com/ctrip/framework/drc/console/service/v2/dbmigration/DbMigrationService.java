package com.ctrip.framework.drc.console.service.v2.dbmigration;

import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.core.http.PageResult;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;

/**
 * @ClassName DbMigrationService
 * @Author haodongPan
 * @Date 2023/8/14 11:48
 * @Version: $
 * optimize to do:support db replication migrate;import status machine;
 */
public interface DbMigrationService {

    boolean abandonTask(Long taskId) throws SQLException;

    // cancel task in init / preStarted / preStarting status
    boolean cancelTask(Long taskId) throws Exception;

    // return null when no dbDrcRelated
    // return taskId when task create; 
    // throw ConsoleException with reason when forbidden
    Pair<String, Long> dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException;
    
    boolean preStartDbMigrationTask(Long taskId) throws SQLException;

    boolean startDbMigrationTask(Long taskId) throws SQLException;

    // pair(tip, status) 
    Pair<String, String> getAndUpdateTaskStatus(Long taskId,boolean ignoreOldMhaDelay);

    PageResult<MigrationTaskTbl> queryByPage(MigrationTaskQuery query);

    void offlineOldDrcConfig(long taskId) throws Exception;

    void rollBackNewDrcConfig(long taskId) throws Exception;

    void deleteReplicator(String mhaName) throws Exception;

    void migrateMhaReplication(String newMhaName, String oldMhaName) throws Exception;

    /**
     * preStart replicator before migrate mhaReplication
     */
    void preStartReplicator(String newMhaName, String oldMhaName) throws Exception;
    
    void checkMhaConfig(String oldMha, String newMha, java.util.Set<String> ignoreConfigName) throws com.ctrip.framework.drc.console.exception.ConsoleException;

}