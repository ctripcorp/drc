package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.platform.dal.dao.DalHints;
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
    private static final String MQ_TYPE = "mq_type";
    private static final String DELETED = "deleted";


    public MessengerGroupTblDao() throws SQLException {
        super(MessengerGroupTbl.class);
    }

    public List<MessengerGroupTbl> queryByMqType(MqType mqType, Integer deleted) throws SQLException {
        if (mqType == null) {
            throw new IllegalArgumentException("queryByMqType but mqType is null");
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .equal(MQ_TYPE, mqType.name(), Types.VARCHAR).and()
                .equal(DELETED, deleted, Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }


    // 1 mha only have 1 messengerGroup with specific mqType
    public MessengerGroupTbl queryByMhaIdAndMqType(Long mhaId, MqType mqType, Integer deleted) throws SQLException {
        if (mhaId == null) {
            throw new IllegalArgumentException("query MessengerGroup By MhaId but mhaId is null");
        }
        if (mqType == null) {
            throw new IllegalArgumentException("query MessengerGroup By MqType but mqType is null");
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .equal(MHA_ID, mhaId, Types.BIGINT).and()
                .equal(MQ_TYPE, mqType.name(), Types.VARCHAR).and()
                .equal(DELETED, deleted, Types.TINYINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }



    public List<MessengerGroupTbl> queryByMhaIdsAndMqType(List<Long> mhaIds, MqType mqType, Integer deleted) throws SQLException {
        if (CollectionUtils.isEmpty(mhaIds) || mqType == null) {
            return Collections.emptyList();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .in(MHA_ID, mhaIds, Types.BIGINT).and()
                .equal(MQ_TYPE, mqType.name(), Types.VARCHAR).and()
                .equal(DELETED, deleted, Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }


    // srcReplicatorGroupId current is useless
    public Long upsertIfNotExist(Long mhaId, Long srcReplicatorGroupId, String gtidExecuted, MqType mqType) throws SQLException {
        if (mhaId == null) {
            throw new IllegalArgumentException("upsertIfNotExist MessengerGroupTbl fail, mhaId is null");
        }
        if (mqType == null) {
            throw new IllegalArgumentException("upsertIfNotExist MessengerGroupTbl fail, mqType is null");
        }
        MessengerGroupTbl mGroupTbl = queryByMhaIdAndMqType(mhaId, mqType, BooleanEnum.FALSE.getCode());
        if (mGroupTbl != null) {
            if (StringUtils.isNotBlank(gtidExecuted)) {
                mGroupTbl.setGtidExecuted(gtidExecuted);
                update(mGroupTbl);
            }
            return mGroupTbl.getId();
        } else {
            MessengerGroupTbl mGroupTblDeleted = queryByMhaIdAndMqType(mhaId, mqType, BooleanEnum.TRUE.getCode());
            if (mGroupTblDeleted != null) {
                mGroupTblDeleted.setDeleted(BooleanEnum.FALSE.getCode());
                mGroupTblDeleted.setGtidExecuted(StringUtils.isBlank(gtidExecuted) ? mGroupTblDeleted.getGtidExecuted() : gtidExecuted);
                update(mGroupTblDeleted);
                return mGroupTblDeleted.getId();
            } else {
                MessengerGroupTbl messengerGroupTbl = new MessengerGroupTbl();
                messengerGroupTbl.setMhaId(mhaId);
                messengerGroupTbl.setMqType(mqType.name());
                messengerGroupTbl.setReplicatorGroupId(srcReplicatorGroupId);
                messengerGroupTbl.setGtidExecuted(gtidExecuted);
                return insertWithReturnId(messengerGroupTbl);
            }
        }
    }


}
