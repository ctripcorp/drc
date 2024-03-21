package com.ctrip.framework.drc.console.dao.v3;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MhaDbReplicationQuery;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@Repository
public class MhaDbReplicationTblDao extends AbstractDao<MhaDbReplicationTbl> {

    private static final String SRC_MHA_DB_MAPPING_ID = "src_mha_db_mapping_id";
    private static final String DST_MHA_DB_MAPPING_ID = "dst_mha_db_mapping_id";
    private static final String DELETED = "deleted";
    private static final String DATA_CHANGE_LAST_TIME = "datachange_lasttime";
    private static final String REPLICATION_TYPE = "replication_type";

    private static final boolean DESCENDING = false;
    public static final String DRC_STATUS = "drc_status";

    public MhaDbReplicationTblDao() throws SQLException {
        super(MhaDbReplicationTbl.class);
    }


    public List<MhaDbReplicationTbl> query(MhaDbReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        this.buildQueryCondition(sqlBuilder, query);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<MhaDbReplicationTbl> queryByPage(MhaDbReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().atPage(query.getPageIndex(), query.getPageSize())
                .orderBy(SRC_MHA_DB_MAPPING_ID, DESCENDING)
                .orderBy(DATA_CHANGE_LAST_TIME, DESCENDING);
        this.buildQueryCondition(sqlBuilder, query);
        return client.query(sqlBuilder, new DalHints());
    }

    public int count(MhaDbReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().selectCount();
        this.buildQueryCondition(sqlBuilder, query);
        return client.count(sqlBuilder, new DalHints()).intValue();
    }

    private void buildQueryCondition(SelectSqlBuilder sqlBuilder, MhaDbReplicationQuery query) throws SQLException {
        sqlBuilder.and()
                .inNullable(ID, query.getIdList(), Types.BIGINT).and()
                .notInNullable(ID, query.getExcludeIdList(), Types.BIGINT).and()
                .inNullable(SRC_MHA_DB_MAPPING_ID, query.getSrcMappingIdList(), Types.BIGINT).and()
                .inNullable(DST_MHA_DB_MAPPING_ID, query.getDstMappingIdList(), Types.BIGINT).and()
                .equalNullable(REPLICATION_TYPE, query.getType(), Types.TINYINT).and()
                .leftBracket()
                .inNullable(SRC_MHA_DB_MAPPING_ID, query.getRelatedMappingList(), Types.BIGINT).or()
                .inNullable(DST_MHA_DB_MAPPING_ID, query.getRelatedMappingList(), Types.BIGINT)
                .rightBracket();
    }


    public Long insertWithReturnId(MhaDbReplicationTbl MhaDbReplicationTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, MhaDbReplicationTbl);
        return (Long) keyHolder.getKey();
    }

    // src_mha_db_mapping_id, dst_mha_db_mapping_id
    // SELECT * FROM mha_db_replication_tbl WHERE (src_mha_db_mapping_id, dst_mha_db_mapping_id) IN ((13119, 13126), (13161, 13189));
    public List<MhaDbReplicationTbl> queryBySamples(List<MhaDbReplicationTbl> sampleList) throws SQLException {
        if (CollectionUtils.isEmpty(sampleList)) {
            return Collections.emptyList();
        }
        return queryBySQL(buildSQL(sampleList));
    }

    private String buildSQL(List<MhaDbReplicationTbl> sampleList) {
        // e.g: (src_mha_db_mapping_id, dst_mha_db_mapping_id, replication_type) IN ((13119, 13126, 0), (13161, 13189, 0));
        List<String> list = sampleList.stream().map(e -> String.format("(%d,%d,%d)", e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId(), e.getReplicationType())).collect(Collectors.toList());
        return String.format("(%s, %s, %s) in (%s) and deleted = 0", SRC_MHA_DB_MAPPING_ID, DST_MHA_DB_MAPPING_ID, REPLICATION_TYPE, String.join(",", list));
    }
}
