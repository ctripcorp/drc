package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName MessengerGroupTblDao
 * @Author haodongPan
 * @Date 2022/10/8 14:23
 * @Version: $
 */
@Repository
public class MessengerGroupTblDao extends AbstractDao<MessengerGroupTbl> {
    
    public MessengerGroupTblDao() throws SQLException {
        super(MessengerGroupTbl.class);
    }

    // 1 mha only have 1 messengerGroup
    public MessengerGroupTbl queryByMhaId(Long mhaId) throws SQLException {
        if (null == mhaId) {
            throw new IllegalArgumentException("query MessengerGroup By MhaId but mhaId is null");
        }
        MessengerGroupTbl sample = new MessengerGroupTbl();
        sample.setMhaId(mhaId);
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        List<MessengerGroupTbl> messengerGroupTbls = queryBy(sample);
        return CollectionUtils.isEmpty(messengerGroupTbls) ? null : messengerGroupTbls.get(0);
    }
}
