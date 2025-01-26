package com.ctrip.framework.drc.console.dao.v3;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerGroupTblV3;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Repository
public class MessengerGroupTblV3Dao extends AbstractDao<MessengerGroupTblV3> {

    private static final String MHA_DB_REPLICATION_ID = "mha_db_replication_id";
    private static final String MQ_TYPE = "mq_type";
    private static final String DELETED = "deleted";

    public MessengerGroupTblV3Dao() throws SQLException {
        super(MessengerGroupTblV3.class);
    }

    public List<MessengerGroupTblV3> queryByMhaDbReplicationIdsAndMqType(List<Long> mhaReplicationIds, MqType mqType) throws SQLException {
        if (CollectionUtils.isEmpty(mhaReplicationIds)) {
            return Collections.emptyList();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(MHA_DB_REPLICATION_ID, mhaReplicationIds, Types.BIGINT)
                .and().equal(MQ_TYPE, mqType.name(), Types.VARCHAR)
        ;
        return client.query(sqlBuilder, new DalHints());
    }

    // srcReplicatorGroupId current is useless
    public void upsert(List<MessengerGroupTblV3> groupTblV3List, MqType mqType) throws SQLException {
        if (CollectionUtils.isEmpty(groupTblV3List)) {
            return;
        }
        List<Long> ids = groupTblV3List.stream().map(MessengerGroupTblV3::getMhaDbReplicationId).collect(Collectors.toList());
        List<MessengerGroupTblV3> existGroup = queryByMhaDbReplicationIdsAndMqType(ids, mqType);
        Map<Long, MessengerGroupTblV3> existMap = existGroup.stream().collect(Collectors.toMap(MessengerGroupTblV3::getMhaDbReplicationId, e -> e));


        List<MessengerGroupTblV3> inserts = Lists.newArrayList();
        List<MessengerGroupTblV3> updates = Lists.newArrayList();
        for (MessengerGroupTblV3 tblV3 : groupTblV3List) {
            MessengerGroupTblV3 exist = existMap.get(tblV3.getMhaDbReplicationId());
            if (exist == null) {
                inserts.add(tblV3);
            } else {
                tblV3.setId(exist.getId());
                if (!Objects.equals(exist.getGtidExecuted(), tblV3.getGtidExecuted()) || !exist.getDeleted().equals(BooleanEnum.FALSE.getCode())) {
                    updates.add(tblV3);
                }
            }
        }

        batchInsertWithReturnId(inserts);
        batchUpdate(updates);
    }

    public void batchInsertWithReturnId(List<MessengerGroupTblV3> groupTblV3List) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insertWithKeyHolder(keyHolder, groupTblV3List);
        List<Number> idList = keyHolder.getIdList();
        int size = groupTblV3List.size();
        for (int i = 0; i < size; i++) {
            groupTblV3List.get(i).setId((Long) idList.get(i));
        }
    }


}
