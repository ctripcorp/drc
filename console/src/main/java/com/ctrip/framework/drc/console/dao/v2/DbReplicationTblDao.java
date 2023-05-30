package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
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

}
