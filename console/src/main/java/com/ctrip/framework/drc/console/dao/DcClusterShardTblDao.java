package com.ctrip.framework.drc.console.dao;

import java.sql.SQLException;
import com.ctrip.framework.drc.console.dao.entity.DcClusterShardTbl;


/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
public class DcClusterShardTblDao extends AbstractDao<DcClusterShardTbl> {

    public DcClusterShardTblDao() throws SQLException {
        super(DcClusterShardTbl.class);
    }
}

