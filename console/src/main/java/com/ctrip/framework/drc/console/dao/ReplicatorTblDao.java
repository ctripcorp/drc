package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
@Repository
public class ReplicatorTblDao extends AbstractDao<ReplicatorTbl> {

    private static final String RESOURCE_ID = "resource_id";
    private static final String DELETED = "deleted";

    public ReplicatorTblDao() throws SQLException {
        super(ReplicatorTbl.class);
    }

    public List<ReplicatorTbl> queryByRGroupIds(List<Long> replicatorGroupIds, Integer deleted) throws SQLException {
        if (CollectionUtils.isEmpty(replicatorGroupIds)) {
            throw new IllegalArgumentException("build sql: query ReplicatorTbls byRGroupIds, but rGroupIds is empty.");
        }
        SelectSqlBuilder builder = new SelectSqlBuilder();
        builder.selectAll().in("relicator_group_id", replicatorGroupIds, Types.BIGINT, false)
                .and().equal("deleted", deleted, Types.TINYINT, false);
        return client.query(builder, new DalHints());
    }

    public List<ReplicatorTbl> queryByResourceIds(List<Long> resourceIds) throws SQLException {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(RESOURCE_ID, resourceIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }

}