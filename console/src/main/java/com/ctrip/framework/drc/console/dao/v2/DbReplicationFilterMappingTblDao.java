package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.springframework.util.CollectionUtils;

/**
 * Created by dengquanliang
 * 2023/5/25 12:00
 */
@Repository
public class DbReplicationFilterMappingTblDao extends AbstractDao<DbReplicationFilterMappingTbl> {

    private static final String DB_REPLICATION_ID = "db_replication_id";
    private static final String MESSENGER_FILTER_ID = "messenger_filter_id";
    private static final String DELETED = "deleted";
    private static final int DEFAULT_NULL_TAG = -1;

    public DbReplicationFilterMappingTblDao() throws SQLException {
        super(DbReplicationFilterMappingTbl.class);
    }

    public List<DbReplicationFilterMappingTbl> queryByDbReplicationId(long dbReplicationId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DB_REPLICATION_ID, dbReplicationId, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationFilterMappingTbl> queryByDbReplicationIds(List<Long> dbReplicationIds) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return Lists.newArrayList();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationFilterMappingTbl> queryMessengerDbReplicationByIds(List<Long> dbReplicationIds) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT)
                .and().notEqual(MESSENGER_FILTER_ID, DEFAULT_NULL_TAG, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }
}
