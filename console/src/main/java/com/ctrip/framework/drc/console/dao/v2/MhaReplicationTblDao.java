package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 12:03
 */
@Repository
public class MhaReplicationTblDao extends AbstractDao<MhaReplicationTbl> {

    private static final String SRC_MHA_ID = "src_mha_id";
    private static final String DST_MHA_ID = "dst_mha_id";
    private static final String DELETED = "deleted";

    public MhaReplicationTblDao() throws SQLException {
        super(MhaReplicationTbl.class);
    }

    public List<MhaReplicationTbl> queryByPage(MhaReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll()
                .atPage(query.getPageIndex(), query.getPageSize())
                .orderBy("src_mha_id", false)
                .inNullable(SRC_MHA_ID, query.getSrcMhaIdList(), Types.BIGINT).and()
                .inNullable(DST_MHA_ID, query.getDesMhaIdList(), Types.BIGINT);
        return client.query(sqlBuilder, new DalHints());
    }

    public int count(MhaReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectCount()
                .inNullable(SRC_MHA_ID, query.getSrcMhaIdList(), Types.BIGINT).and()
                .inNullable(DST_MHA_ID, query.getDesMhaIdList(), Types.BIGINT);
        return client.count(sqlBuilder, new DalHints()).intValue();
    }


    public MhaReplicationTbl queryByMhaId(Long srcMhaId, Long dstMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT).and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public MhaReplicationTbl queryByMhaId(Long srcMhaId, Long dstMhaId, Integer deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT)
                .and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public Long insertWithReturnId(MhaReplicationTbl mhaReplicationTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, mhaReplicationTbl);
        return (Long) keyHolder.getKey();
    }
}
