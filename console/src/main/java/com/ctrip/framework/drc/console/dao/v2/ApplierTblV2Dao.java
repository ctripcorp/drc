package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
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
 * Created by dengquanliang
 * 2023/5/25 11:57
 */
@Repository
public class ApplierTblV2Dao extends AbstractDao<ApplierTblV2> {

    private static final String APPLIER_GROUP_ID = "applier_group_id";
    private static final String DELETED = "deleted";
    private static final String RESOURCE_ID = "resource_id";
    private static final String DELETED = "deleted";

    public ApplierTblV2Dao() throws SQLException {
        super(ApplierTblV2.class);
    }

    public List<ApplierTblV2> queryByApplierGroupId(Long applierGroupId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(APPLIER_GROUP_ID, applierGroupId, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ApplierTblV2> queryByApplierGroupId(Long applierGroupId, Integer deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DELETED, deleted, Types.TINYINT).and().equal(APPLIER_GROUP_ID, applierGroupId, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ApplierTblV2> queryByApplierGroupIds(List<Long> applierGroupIds, int deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().inNullable(APPLIER_GROUP_ID, applierGroupIds, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ApplierTblV2> queryByResourceIds(List<Long> resourceIds) throws SQLException {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(RESOURCE_ID, resourceIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }
}
