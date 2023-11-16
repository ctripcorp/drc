package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictAutoHandleTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/30 20:15
 */
@Repository
public class ConflictAutoHandleTblDao extends AbstractDao<ConflictAutoHandleTbl> {

    private static final String BATCH_ID = "batch_id";

    public ConflictAutoHandleTblDao() throws SQLException {
        super(ConflictAutoHandleTbl.class);
    }

    public List<ConflictAutoHandleTbl> queryByBatchId(Long batchId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(BATCH_ID, batchId, Types.BIGINT);
        return queryList(sqlBuilder);
    }
}
