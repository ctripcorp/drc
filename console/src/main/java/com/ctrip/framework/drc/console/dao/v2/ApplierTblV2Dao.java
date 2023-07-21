package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 11:57
 */
@Repository
public class ApplierTblV2Dao extends AbstractDao<ApplierTblV2> {

    private static final String APPLIER_GROUP_ID = "applier_group_id";

    public ApplierTblV2Dao() throws SQLException {
        super(ApplierTblV2.class);
    }

    public List<ApplierTblV2> queryByApplierGroupId(Long applierGroupId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(APPLIER_GROUP_ID, applierGroupId, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }
}
