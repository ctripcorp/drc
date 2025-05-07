package com.ctrip.framework.drc.console.dao.v3;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ApplierGroupTblV3Dao extends AbstractDao<ApplierGroupTblV3> {

    private static final String MHA_DB_REPLICATION_ID = "mha_db_replication_id";
    private static final String GTID_INIT = "gtid_init";
    private static final String CONCURRENCY = "concurrency";
    private static final String DELETED = "deleted";

    public ApplierGroupTblV3Dao() throws SQLException {
        super(ApplierGroupTblV3.class);
    }

    public List<ApplierGroupTblV3> queryByMhaDbReplicationIds(List<Long> mhaReplicationIds) throws SQLException {
        if (CollectionUtils.isEmpty(mhaReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(MHA_DB_REPLICATION_ID, mhaReplicationIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public ApplierGroupTblV3 queryByMhaDbReplicationId(long mhaDbReplicationId, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(MHA_DB_REPLICATION_ID, mhaDbReplicationId, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return queryOne(sqlBuilder);
    }

    public ApplierGroupTblV3 queryByMhaDbReplicationId(long mhaDbReplicationId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(MHA_DB_REPLICATION_ID, mhaDbReplicationId, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryOne(sqlBuilder);
    }



    public void batchInsertWithReturnId(List<ApplierGroupTblV3> applierGroupTblV3s) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insertWithKeyHolder(keyHolder, applierGroupTblV3s);
        List<Number> idList = keyHolder.getIdList();
        int size = applierGroupTblV3s.size();
        for (int i = 0; i <size; i++) {
            applierGroupTblV3s.get(i).setId((Long) idList.get(i));
        }
    }

    @Override
    public int[] batchUpdate(List<ApplierGroupTblV3> daoPojos) throws SQLException {
        return super.batchUpdate(new DalHints().updateNullField(GTID_INIT, CONCURRENCY), daoPojos);
    }

    public Long insertOrReCover(Long mhaDbReplicationId, String gtidInit) throws SQLException {
        if (mhaDbReplicationId == null) {
            throw new IllegalArgumentException("insertOrReCover ApplierGroupTblV3, mhaDbReplicationId is null");
        }
        ApplierGroupTblV3 aGroup = queryByMhaDbReplicationId(mhaDbReplicationId);
        if (aGroup != null) {
            if (aGroup.getDeleted() == 1) {
                aGroup.setDeleted(0);
                aGroup.setGtidInit(StringUtils.isBlank(gtidInit) ? aGroup.getGtidInit() : gtidInit);
                update(aGroup);
            }
            return aGroup.getId();
        } else {
            ApplierGroupTblV3 newGroup = new ApplierGroupTblV3();
            newGroup.setMhaDbReplicationId(mhaDbReplicationId);
            newGroup.setGtidInit(StringUtils.isBlank(gtidInit) ? null : gtidInit);
            return insertWithReturnId(newGroup);
        }
    }
}
