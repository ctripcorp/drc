package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName MessengerTblDao
 * @Author haodongPan
 * @Date 2022/10/8 14:24
 * @Version: $
 */
@Repository
public class MessengerTblDao extends AbstractDao<MessengerTbl> {

    private static final String RESOURCE_ID = "resource_id";
    private static final String GROUP_ID = "group_id";
    private static final String DELETED = "deleted";

    public MessengerTblDao() throws SQLException {
        super(MessengerTbl.class);
    }

    public List<MessengerTbl> queryByGroupId(Long groupId) throws SQLException {
        if (null == groupId) {
            throw new IllegalArgumentException("queryMessengerTblsByGroupId but groupId is null");
        }
        MessengerTbl sample = new MessengerTbl();
        sample.setMessengerGroupId(groupId);
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        return queryBy(sample);
    }

    public Long insertMessenger(int port, Long resourceId, Long messengerGroupId) throws SQLException {
        MessengerTbl pojo = new MessengerTbl();
        pojo.setPort(port);
        pojo.setResourceId(resourceId);
        pojo.setMessengerGroupId(messengerGroupId);
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, pojo);
        return (Long) keyHolder.getKey();
    }

    public List<MessengerTbl> queryByResourceIds(List<Long> resourceIds) throws SQLException {
        if (CollectionUtils.isEmpty(resourceIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(RESOURCE_ID, resourceIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }


    public List<MessengerTbl> queryByGroupIds(List<Long> groupIds) throws SQLException {
        if (CollectionUtils.isEmpty(groupIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(GROUP_ID, groupIds, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return queryList(sqlBuilder);
    }
    
    
}
