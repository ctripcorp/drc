package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictAutoHandleBatchTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/10/30 20:18
 */
@Repository
public class ConflictAutoHandleBatchTblDao extends AbstractDao<ConflictAutoHandleBatchTbl> {

    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";

    public ConflictAutoHandleBatchTblDao() throws SQLException {
        super(ConflictAutoHandleBatchTbl.class);
    }

    public List<ConflictAutoHandleBatchTbl> queryByDb(String dbName, String tableName) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (StringUtils.isNotBlank(dbName)) {
            sqlBuilder.and().like(DB_NAME, dbName, MatchPattern.CONTAINS, Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(tableName)) {
            sqlBuilder.and().like(TABLE_NAME, tableName, MatchPattern.CONTAINS, Types.VARCHAR);
        }

        return queryList(sqlBuilder);
    }
}
