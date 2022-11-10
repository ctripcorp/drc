package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
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

    // 1 mha only have 1 messengerGroup when messenger function is db -> mq
    public MessengerGroupTbl queryByMhaId(Long mhaId,Integer deleted) throws SQLException {
        if (null == mhaId) {
            throw new IllegalArgumentException("query MessengerGroup By MhaId but mhaId is null");
        }
        MessengerGroupTbl sample = new MessengerGroupTbl();
        sample.setMhaId(mhaId);
        sample.setDeleted(deleted);
        List<MessengerGroupTbl> messengerGroupTbls = queryBy(sample);
        return CollectionUtils.isEmpty(messengerGroupTbls) ? null : messengerGroupTbls.get(0);
    }

    // srcReplicatorGroupId current is useless
    public Long upsertIfNotExist(Long mhaId,Long srcReplicatorGroupId,String gtidExecuted) throws SQLException {
        MessengerGroupTbl mGroupTbl =
                queryByMhaId(mhaId,BooleanEnum.FALSE.getCode());
        if (mGroupTbl != null) {
            if(StringUtils.isNotBlank(gtidExecuted)) {
                mGroupTbl.setGtidExecuted(gtidExecuted);
                update(mGroupTbl);
            }
            return mGroupTbl.getId();
        } else {
            MessengerGroupTbl mGroupTblDeleted =
                    queryByMhaId(mhaId, BooleanEnum.TRUE.getCode());
            if (mGroupTblDeleted != null) {
                mGroupTblDeleted.setDeleted(BooleanEnum.FALSE.getCode());
                mGroupTblDeleted.setGtidExecuted(gtidExecuted);
                update(mGroupTblDeleted);
                return mGroupTblDeleted.getId();
            } else {
                KeyHolder keyHolder = new KeyHolder();
                MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
                messengerGroupTbl.setMhaId(mhaId);
                messengerGroupTbl.setReplicatorGroupId(srcReplicatorGroupId);
                messengerGroupTbl.setGtidExecuted(gtidExecuted);
                insert(new DalHints(), keyHolder, messengerGroupTbl);
                return (Long) keyHolder.getKey();
            }
        }
    }


   
}
