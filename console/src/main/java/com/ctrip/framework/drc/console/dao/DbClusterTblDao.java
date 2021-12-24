package com.ctrip.framework.drc.console.dao;

import java.sql.SQLException;
import com.ctrip.framework.drc.console.dao.entity.DbClusterTbl;


/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
public class DbClusterTblDao extends AbstractDao<DbClusterTbl> {

    public DbClusterTblDao() throws SQLException {
        super(DbClusterTbl.class);
    }
}

