package com.ctrip.framework.drc.console.dao;

import java.sql.SQLException;
import com.ctrip.framework.drc.console.dao.entity.DcClusterTbl;


/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
public class DcClusterTblDao extends AbstractDao<DcClusterTbl> {

    public DcClusterTblDao() throws SQLException {
        super(DcClusterTbl.class);
    }
}

