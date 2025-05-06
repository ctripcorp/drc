package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DbReplicationRouteMappingTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2025/4/1 14:33
 */
@Repository
public class DbReplicationRouteMappingTblDao extends AbstractDao<DbReplicationRouteMappingTbl> {

    private static final String ROUTE_ID = "route_id";
    private static final String MHA_ID = "mha_id";
    private static final String MHA_DB_REPLICATION_ID = "mha_db_replication_id";

    public DbReplicationRouteMappingTblDao() throws SQLException {
        super(DbReplicationRouteMappingTbl.class);
    }

    public List<DbReplicationRouteMappingTbl> queryByRouteId(long routeId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(ROUTE_ID, routeId, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<DbReplicationRouteMappingTbl> queryByMhaId(long mhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(MHA_ID, mhaId, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<DbReplicationRouteMappingTbl> queryByMhaIds(long routeId, List<Long> mhaIds) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(ROUTE_ID, routeId, Types.BIGINT)
                .and().in(MHA_ID, mhaIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<DbReplicationRouteMappingTbl> queryByMhaDbReplicationIds(long routeId, List<Long> mhaDbReplicationIds) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(ROUTE_ID, routeId, Types.BIGINT)
                .and().in(MHA_DB_REPLICATION_ID, mhaDbReplicationIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<DbReplicationRouteMappingTbl> queryByMhaDbReplicationIds(List<Long> mhaDbReplicationIds) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(MHA_DB_REPLICATION_ID, mhaDbReplicationIds, Types.BIGINT);
        return queryList(sqlBuilder);
    }
}
