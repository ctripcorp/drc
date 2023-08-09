package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/5/25 11:54
 */
@Repository
public class ApplierGroupTblV2Dao extends AbstractDao<ApplierGroupTblV2> {

    public ApplierGroupTblV2Dao() throws SQLException {
        super(ApplierGroupTblV2.class);
    }

    public ApplierGroupTblV2 queryByReplicationId(Long mhaReplicationId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectAll().and().equal("mha_replication_id", mhaReplicationId, Types.BIGINT);
        return client.queryFirst(sqlBuilder, new DalHints());

    }
}
