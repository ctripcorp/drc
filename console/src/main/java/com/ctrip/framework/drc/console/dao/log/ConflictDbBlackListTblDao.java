package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/11/15 18:35
 */
@Repository
public class ConflictDbBlackListTblDao extends AbstractDao<ConflictDbBlackListTbl> {

    private static final String DB_FILTER = "db_filter";

    public ConflictDbBlackListTblDao() throws SQLException {
        super(ConflictDbBlackListTbl.class);
    }

    public List<ConflictDbBlackListTbl> queryByDbFilter(String dbFilter) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(DB_FILTER, dbFilter, Types.VARCHAR);
        return queryList(sqlBuilder);
    }
}
