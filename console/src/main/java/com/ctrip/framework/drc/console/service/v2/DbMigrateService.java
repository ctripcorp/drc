package com.ctrip.framework.drc.console.service.v2;

/**
 * Created by dengquanliang
 * 2023/8/21 10:51
 */
public interface DbMigrateService {

    void offlineOldDrcConfig(long taskId) throws Exception;

    void rollBackNewDrcConfig(long taskId) throws Exception;
}
