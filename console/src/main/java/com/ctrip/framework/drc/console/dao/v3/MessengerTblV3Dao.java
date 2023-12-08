package com.ctrip.framework.drc.console.dao.v3;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerTblV3;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Repository
public class MessengerTblV3Dao extends AbstractDao<MessengerTblV3> {

    private static final String RESOURCE_ID = "resource_id";
    private static final String GROUP_ID = "messenger_group_id";
    private static final String DELETED = "deleted";

    public MessengerTblV3Dao() throws SQLException {
        super(MessengerTblV3.class);
    }

    public List<MessengerTblV3> queryByGroupId(Long groupId) throws SQLException {
        if (null == groupId) {
            throw new IllegalArgumentException("queryMessengerTblV3sByGroupId but groupId is null");
        }
        MessengerTblV3 sample = new MessengerTblV3();
        sample.setMessengerGroupId(groupId);
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        return queryBy(sample);
    }

    public List<MessengerTblV3> queryByResourceIds(List<Long> resourceIds) throws SQLException {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(RESOURCE_ID, resourceIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }


    public List<MessengerTblV3> queryByGroupIds(List<Long> groupIds) throws SQLException {
        if (CollectionUtils.isEmpty(groupIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(GROUP_ID, groupIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }


}
