package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictDbBlackListTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.log.ConflictDbBlacklistQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
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
    private static final String TYPE = "type";
    private static final String DATACHANGE_LASTTIME = "datachange_lasttime";
    private static final String DELETED = "deleted";

    public ConflictDbBlackListTblDao() throws SQLException {
        super(ConflictDbBlackListTbl.class);
    }

    public List<ConflictDbBlackListTbl> queryBy(String dbFilter, Integer type) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(DB_FILTER, dbFilter, Types.VARCHAR);
        sqlBuilder.and().equal(TYPE, type, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<ConflictDbBlackListTbl> queryByDbFilter(String dbFilter) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(DB_FILTER, dbFilter, Types.VARCHAR);
        return queryList(sqlBuilder);
    }

    public List<ConflictDbBlackListTbl> queryByType(Integer type) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(TYPE, type, Types.TINYINT);
        return queryList(sqlBuilder);
    }

    public List<ConflictDbBlackListTbl> query(ConflictDbBlacklistQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectCount();
        int count = client.count(sqlBuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(DATACHANGE_LASTTIME, false);
        return queryList(sqlBuilder);
    }

    private SelectSqlBuilder buildSqlBuilder(ConflictDbBlacklistQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
        if (StringUtils.isNotBlank(param.getDbFilter())) {
            sqlBuilder.and().like(DB_FILTER, param.getDbFilter(), MatchPattern.CONTAINS, Types.VARCHAR);
        }
        if (param.getType() != null) {
            sqlBuilder.and().equal(TYPE, param.getType(), Types.TINYINT);
        }
        return sqlBuilder;
    }
}
