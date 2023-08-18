package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
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

    private static final String MHA_REPLICATION_ID = "mha_replication_id";
    private static final String DELETED = "deleted";

    public ApplierGroupTblV2Dao() throws SQLException {
        super(ApplierGroupTblV2.class);
    }

    /**
     * mhaReplicationId can not be -1L
     */
    public ApplierGroupTblV2 queryByMhaReplicationId(long mhaReplicationId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(MHA_REPLICATION_ID, mhaReplicationId, Types.BIGINT);
        return queryOne(sqlBuilder);
    }

    public ApplierGroupTblV2 queryByMhaReplicationId(long mhaReplicationId, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(MHA_REPLICATION_ID, mhaReplicationId, Types.BIGINT)
        .and().equal(DELETED, deleted, Types.BIGINT);
        return queryOne(sqlBuilder);
    }
}
