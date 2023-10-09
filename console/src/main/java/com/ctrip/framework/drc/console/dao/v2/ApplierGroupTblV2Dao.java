package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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

    public List<ApplierGroupTblV2> queryByMhaReplicationIds(List<Long> mhaReplicationIds) throws SQLException {
        if (CollectionUtils.isEmpty(mhaReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(MHA_REPLICATION_ID, mhaReplicationIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }
    
    public Long insertOrReCover(Long mhaReplicationId, String gtidInit) throws SQLException {
        if (mhaReplicationId == null) {
            throw new IllegalArgumentException("insertOrReCover ApplierGroupTblV2, mhaReplicationId is null");
        }
        ApplierGroupTblV2 aGroup = queryByMhaReplicationId(mhaReplicationId);
        if (aGroup != null) {
            if (aGroup.getDeleted() == 1) {
                aGroup.setDeleted(0);
                aGroup.setGtidInit(StringUtils.isBlank(gtidInit) ? aGroup.getGtidInit() : gtidInit);
                update(aGroup);
            }
            return aGroup.getId();
        } else {
            ApplierGroupTblV2 newGroup = new ApplierGroupTblV2();
            newGroup.setMhaReplicationId(mhaReplicationId);
            newGroup.setGtidInit(StringUtils.isBlank(gtidInit) ? null : gtidInit);
            return insertWithReturnId(newGroup);
        }
    }
    
    
}
