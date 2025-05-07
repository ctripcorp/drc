package com.ctrip.framework.drc.console.dao.v3;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ApplierTblV3Dao extends AbstractDao<ApplierTblV3> {

    private static final String APPLIER_GROUP_ID = "applier_group_id";
    private static final String DELETED = "deleted";
    private static final String RESOURCE_ID = "resource_id";

    public ApplierTblV3Dao() throws SQLException {
        super(ApplierTblV3.class);
    }


    public List<ApplierTblV3> queryByApplierGroupId(Long applierGroupId, Integer deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(DELETED, deleted, Types.TINYINT).and().equal(APPLIER_GROUP_ID, applierGroupId, Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ApplierTblV3> queryByApplierGroupIds(List<Long> applierGroupIds, int deleted) throws SQLException {
        if (CollectionUtils.isEmpty(applierGroupIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(APPLIER_GROUP_ID, applierGroupIds, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<ApplierTblV3> queryByResourceIds(List<Long> resourceIds) throws SQLException {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(RESOURCE_ID, resourceIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }

}
