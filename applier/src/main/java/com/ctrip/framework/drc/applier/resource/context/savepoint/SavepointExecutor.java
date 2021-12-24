package com.ctrip.framework.drc.applier.resource.context.savepoint;

import java.sql.SQLException;

/**
 * @Author limingdong
 * @create 2021/2/1
 */
public interface SavepointExecutor {

    boolean executeSavepoint(String identifier) throws SQLException;

    boolean rollbackToSavepoint(String identifier) throws SQLException;

}
