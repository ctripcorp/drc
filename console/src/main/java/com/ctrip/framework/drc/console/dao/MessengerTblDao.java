package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName MessengerTblDao
 * @Author haodongPan
 * @Date 2022/10/8 14:24
 * @Version: $
 */
@Repository
public class MessengerTblDao extends AbstractDao<MessengerTbl> {
    
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
}
