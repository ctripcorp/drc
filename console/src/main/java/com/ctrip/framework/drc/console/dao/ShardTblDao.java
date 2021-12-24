package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ApplierUploadLogTbl;
import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.sqlbuilder.*;
import java.sql.SQLException;
import java.util.List;
import com.ctrip.framework.drc.console.dao.entity.ShardTbl;

import com.ctrip.platform.dal.dao.helper.DalDefaultJpaParser;

/**
 * @author wjx王继欣
 * @date 2020-01-20
 */
public class ShardTblDao extends AbstractDao<ShardTbl> {

    public ShardTblDao() throws SQLException {
        super(ShardTbl.class);
    }
}

