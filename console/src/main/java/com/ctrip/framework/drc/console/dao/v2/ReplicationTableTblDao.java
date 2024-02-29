package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.ReplicationTableTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2024/1/23 16:31
 */
@Repository
public class ReplicationTableTblDao extends AbstractDao<ReplicationTableTbl> {

    private static final String SRC_MHA = "src_mha";
    private static final String DST_MHA = "dst_mha";
    private static final String DB_REPLICATION_ID = "db_replication_id";
    private static final String EFFECTIVE_STATUS = "effective_status";
    private static final String DELETED = "deleted";
    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";
    private static final String EXISTING_DATA_STATUS = "existing_data_status";

    public ReplicationTableTblDao() throws SQLException {
        super(ReplicationTableTbl.class);
    }

    public void deleteAll() throws SQLException {
        client.delete("id >= ?", new DalHints(), 1L);
    }

    // include deleted
    public List<ReplicationTableTbl> queryByDbReplicationIds(List<Long> dbReplicationIds) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<ReplicationTableTbl> queryByDbReplicationIds(List<Long> dbReplicationIds, Integer deleted) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    // include deleted
    public List<ReplicationTableTbl> queryByDbReplicationIds(List<Long> dbReplicationIds, int effectiveStatus) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT)
                .and().equal(EFFECTIVE_STATUS, effectiveStatus, Types.TINYINT);
        return queryList(sqlBuilder);
    }


    public List<ReplicationTableTbl> queryExistByDbReplicationIds(List<Long> dbReplicationIds, int effectiveStatus) throws SQLException {
        if (CollectionUtils.isEmpty(dbReplicationIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(DB_REPLICATION_ID, dbReplicationIds, Types.BIGINT)
                .and().equal(EFFECTIVE_STATUS, effectiveStatus, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<ReplicationTableTbl> queryByDbReplicationIds(String srcMha, String dstMha, int effectiveStatus) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(SRC_MHA, srcMha, Types.VARCHAR)
                .and().equal(DST_MHA, dstMha, Types.VARCHAR)
                .and().equal(EFFECTIVE_STATUS, effectiveStatus, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<ReplicationTableTbl> queryByDbName(String dbName, String tableName, int existingDataStatus) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(DB_NAME, dbName, Types.VARCHAR)
                .and().equal(TABLE_NAME, tableName, Types.VARCHAR)
                .and().equal(EXISTING_DATA_STATUS, existingDataStatus, Types.TINYINT);
        return queryList(sqlBuilder);
    }

}
