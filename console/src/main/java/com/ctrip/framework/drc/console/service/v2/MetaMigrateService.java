package com.ctrip.framework.drc.console.service.v2;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/6/5 16:51
 */
public interface MetaMigrateService {

    void migrateMhaTbl() throws Exception;

    void migrateMhaReplication() throws Exception;
}
