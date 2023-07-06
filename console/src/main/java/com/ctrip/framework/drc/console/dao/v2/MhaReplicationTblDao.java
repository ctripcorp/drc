package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dengquanliang
 * 2023/5/25 12:03
 */
@Repository
public class MhaReplicationTblDao extends AbstractDao<MhaReplicationTbl> {

    private static final String SRC_MHA_ID = "src_mha_id";
    private static final String DST_MHA_ID = "dst_mha_id";

    public MhaReplicationTblDao() throws SQLException {
        super(MhaReplicationTbl.class);
    }

    public MhaReplicationTbl queryByMhaId(Long srcMhaId, Long dstMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT).and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public Long insertWithReturnId(MhaReplicationTbl mhaReplicationTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, mhaReplicationTbl);
        return (Long) keyHolder.getKey();
    }
}
