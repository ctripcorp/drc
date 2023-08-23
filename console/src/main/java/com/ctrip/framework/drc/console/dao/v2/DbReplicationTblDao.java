package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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

    public List<DbReplicationTbl> queryMappingIds(List<Long> mappingIds) throws SQLException {
        if (CollectionUtils.isEmpty(mappingIds)) {
            return new ArrayList<>();
        }
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().inNullable(SRC_MHA_DB_MAPPING_ID, mappingIds, Types.BIGINT)
                .and().inNullable(DST_MHA_DB_MAPPING_ID, mappingIds, Types.BIGINT).and()
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

    public void batchInsertWithReturnId(List<DbReplicationTbl> dbReplicationTbls) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insertWithKeyHolder(keyHolder, dbReplicationTbls);
        List<Number> idList = keyHolder.getIdList();
        int size = dbReplicationTbls.size();
        for (int i = 0; i <size; i++) {
            dbReplicationTbls.get(i).setId((Long) idList.get(i));
        }
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

}
