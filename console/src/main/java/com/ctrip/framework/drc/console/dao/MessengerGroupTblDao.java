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
import java.util.Collections;
import java.util.List;

/**
 * @ClassName MessengerGroupTblDao
 * @Author haodongPan
 * @Date 2022/10/8 14:23
 * @Version: $
 */
@Repository
public class MessengerGroupTblDao extends AbstractDao<MessengerGroupTbl> {

    private static final String MHA_ID = "mha_id";
    private static final String DELETED = "deleted";

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

    public List<MessengerGroupTbl> queryByMhaIds(List<Long> mhaIds,Integer deleted) throws SQLException {
        if (CollectionUtils.isEmpty(mhaIds)) {
            return Collections.emptyList();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .in(MHA_ID, mhaIds, Types.BIGINT).and()
                .equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    // srcReplicatorGroupId current is useless
    public Long upsertIfNotExist(Long mhaId,Long srcReplicatorGroupId,String gtidExecuted) throws SQLException {
        if (null == mhaId) {
            throw new IllegalArgumentException("upsertIfNotExist MessengerGroupTbl but mhaId is null");
        }
        MessengerGroupTbl mGroupTbl = queryByMhaId(mhaId,BooleanEnum.FALSE.getCode());
        if (mGroupTbl != null) {
            if(StringUtils.isNotBlank(gtidExecuted)) {
                mGroupTbl.setGtidExecuted(gtidExecuted);
                update(mGroupTbl);
            }
            return mGroupTbl.getId();
        } else {
            MessengerGroupTbl mGroupTblDeleted = queryByMhaId(mhaId, BooleanEnum.TRUE.getCode());
            if (mGroupTblDeleted != null) {
                mGroupTblDeleted.setDeleted(BooleanEnum.FALSE.getCode());
                mGroupTblDeleted.setGtidExecuted(StringUtils.isBlank(gtidExecuted) ? mGroupTblDeleted.getGtidExecuted() : gtidExecuted);
                update(mGroupTblDeleted);
                return mGroupTblDeleted.getId();
            } else {
                MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
                messengerGroupTbl.setMhaId(mhaId);
                messengerGroupTbl.setReplicatorGroupId(srcReplicatorGroupId);
                messengerGroupTbl.setGtidExecuted(gtidExecuted);
                return insertWithReturnId(messengerGroupTbl);
            }
        }
    }


   
}
