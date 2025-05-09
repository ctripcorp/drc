package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.v2.MqReplicationQuery;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/25 12:01
 */
@Repository
public class DbReplicationTblDao extends AbstractDao<DbReplicationTbl> {

    private static final String SRC_MHA_DB_MAPPING_ID = "src_mha_db_mapping_id";
    private static final String DST_MHA_DB_MAPPING_ID = "dst_mha_db_mapping_id";
    private static final String SRC_LOGIC_TABLE_NAME = "src_logic_table_name";
    private static final String DST_LOGIC_TABLE_NAME = "dst_logic_table_name";
    private static final String REPLICATION_TYPE = "replication_type";
    private static final String DELETED = "deleted";

    public DbReplicationTblDao() throws SQLException {
        super(DbReplicationTbl.class);
    }

    public List<DbReplicationTbl> queryBySrcMappingIds(List<Long> srcMappingIds, int replicationType) throws SQLException {
        if (CollectionUtils.isEmpty(srcMappingIds)) {
            return new ArrayList<>();
        }

        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(SRC_MHA_DB_MAPPING_ID, srcMappingIds, Types.BIGINT)
                .and().equal(REPLICATION_TYPE, replicationType, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryByDestMappingIds(List<Long> destMappingIds, int replicationType) throws SQLException {
        if (CollectionUtils.isEmpty(destMappingIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(DST_MHA_DB_MAPPING_ID, destMappingIds, Types.BIGINT)
                .and().equal(REPLICATION_TYPE, replicationType, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryByRelatedMappingIds(List<Long> srcOrDestMappingIds, int replicationType) throws SQLException {
        if (CollectionUtils.isEmpty(srcOrDestMappingIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .leftBracket()
                .in(SRC_MHA_DB_MAPPING_ID, srcOrDestMappingIds, Types.BIGINT)
                .or()
                .in(DST_MHA_DB_MAPPING_ID, srcOrDestMappingIds, Types.BIGINT)
                .rightBracket()
                .and().equal(REPLICATION_TYPE, replicationType, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryMappingIds(List<Long> mappingIds) throws SQLException {
        if (CollectionUtils.isEmpty(mappingIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .leftBracket()
                .in(SRC_MHA_DB_MAPPING_ID, mappingIds, Types.BIGINT)
                .or()
                .in(DST_MHA_DB_MAPPING_ID, mappingIds, Types.BIGINT)
                .rightBracket()
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryByMappingIds(List<Long> srcMappingIds, List<Long> dstMappingIds, int replicationType) throws SQLException {
        if (CollectionUtils.isEmpty(srcMappingIds) || CollectionUtils.isEmpty(dstMappingIds)) {
            return new ArrayList<>();
        }

        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().in(SRC_MHA_DB_MAPPING_ID, srcMappingIds, Types.BIGINT)
                .and().in(DST_MHA_DB_MAPPING_ID, dstMappingIds, Types.BIGINT)
                .and().equal(REPLICATION_TYPE, replicationType, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryBy(List<Long> srcMappingMhaDbIds, String srcLogicTableName, String dstLogicTableName, int replicationType) throws SQLException {
        if (CollectionUtils.isEmpty(srcMappingMhaDbIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().in(SRC_MHA_DB_MAPPING_ID, srcMappingMhaDbIds, Types.BIGINT)
                .and().equal(SRC_LOGIC_TABLE_NAME, srcLogicTableName, Types.VARCHAR)
                .and().equal(DST_LOGIC_TABLE_NAME, dstLogicTableName, Types.VARCHAR)
                .and().equal(REPLICATION_TYPE, replicationType, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<DbReplicationTbl> batchInsertWithReturnId(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insertWithKeyHolder(keyHolder, dbReplicationTbls);
        List<Number> idList = keyHolder.getIdList();
        int size = dbReplicationTbls.size();
        for (int i = 0; i < size; i++) {
            dbReplicationTbls.get(i).setId((Long) idList.get(i));
        }
        return dbReplicationTbls;
    }

    public List<DbReplicationTbl> queryByDstLogicTableName(String dstLogicTableName, int replicationType) throws SQLException {
        if (StringUtils.isBlank(dstLogicTableName)) {
            return new ArrayList<>();
        }

        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectAll()
                .and().equal(DST_LOGIC_TABLE_NAME, dstLogicTableName, Types.VARCHAR)
                .and().equal(REPLICATION_TYPE, replicationType, Types.BIGINT)
                .and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public List<DbReplicationTbl> queryBySamples(List<DbReplicationTbl> samples) throws SQLException {
        if (CollectionUtils.isEmpty(samples)) {
            return Collections.emptyList();
        }
        return queryBySQL(buildSQL(samples));
    }


    private String buildSQL(List<DbReplicationTbl> samples) {
        // e.g: (src_mha_db_mapping_id, dst_mha_db_mapping_id, replication_type) IN ((13119, 13126, 0), (13161, 13189, 0));
        List<String> list = samples.stream().map(e -> String.format("(%d,%d,%d)", e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId(), e.getReplicationType())).collect(Collectors.toList());
        return String.format("(%s, %s, %s) in (%s) and deleted = 0", SRC_MHA_DB_MAPPING_ID, DST_MHA_DB_MAPPING_ID, REPLICATION_TYPE, String.join(",", list));
    }

    public List<DbReplicationTbl> queryByPage(MqReplicationQuery query) throws SQLException  {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().atPage(query.getPageIndex(), query.getPageSize());
        this.buildQueryCondition(sqlBuilder, query);
        return client.query(sqlBuilder, new DalHints());
    }

    public int count(MqReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().selectCount();
        this.buildQueryCondition(sqlBuilder, query);
        return client.count(sqlBuilder, new DalHints()).intValue();
    }

    private void buildQueryCondition(SelectSqlBuilder sqlBuilder, MqReplicationQuery query) throws SQLException {
        sqlBuilder.and()
                .equal(REPLICATION_TYPE, query.getReplicationType(), Types.TINYINT);

        sqlBuilder.and()
                .inNullable(SRC_MHA_DB_MAPPING_ID, query.getSrcMhaDbMappingIdList(), Types.BIGINT);

        if (query.isQueryOtter()){
            sqlBuilder.and().leftBracket()
                    .like(DST_LOGIC_TABLE_NAME, "otter", MatchPattern.CONTAINS,Types.VARCHAR).or()
                    .not().like(DST_LOGIC_TABLE_NAME, ".binlog", MatchPattern.END_WITH, Types.VARCHAR)
                    .rightBracket();
        }

        if (!CollectionUtils.isEmpty(query.getSrcTableLikePatterns())) {
            sqlBuilder.and().leftBracket();
            for (String srcTableLikePattern : query.getSrcTableLikePatterns()) {
                sqlBuilder.or()
                        .likeNullable(SRC_LOGIC_TABLE_NAME, srcTableLikePattern, MatchPattern.CONTAINS, Types.VARCHAR);
            }
            sqlBuilder.rightBracket();
        }

        if (!CollectionUtils.isEmpty(query.getTopicLikePatterns())) {
            sqlBuilder.and().leftBracket();
            for (String topicLikePattern : query.getTopicLikePatterns()) {
                sqlBuilder.or()
                        .likeNullable(DST_LOGIC_TABLE_NAME, topicLikePattern, MatchPattern.CONTAINS, Types.VARCHAR);
            }
            sqlBuilder.rightBracket();
        }
    }
}
