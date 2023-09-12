package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.v2.DbQuery;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class DbTblDao extends AbstractDao<DbTbl> {

	private static final String ID = "id";
	private static final String DELETED = "deleted";

	public DbTblDao() throws SQLException {
		super(DbTbl.class);
	}

    public List<DbTbl> queryByDbNames(List<String> dbNames) throws SQLException {
		if (CollectionUtils.isEmpty(dbNames)) {
			throw new IllegalArgumentException("build sql: query DbTbl queryByDbNames, but dbNames is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().in("db_name", dbNames, Types.VARCHAR, false);
		return client.query(builder,new DalHints());
    }

	public DbTbl queryByDbName(String dbName) throws SQLException {
		if (StringUtils.isEmpty(dbName)) {
			throw new IllegalArgumentException("dbName is blank");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("db_name", dbName, Types.VARCHAR, false);
		return client.queryFirst(builder, new DalHints());
	}

    public List<DbTbl> queryByPage(DbQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().atPage(query.getPageIndex(), query.getPageSize())
                .orderBy(ID, false);
        sqlBuilder.and().likeNullable("db_name", query.getLikeByDbNameFromBeginning(),  MatchPattern.BEGIN_WITH,Types.VARCHAR);
        return client.query(sqlBuilder, new DalHints());
    }

	public List<DbTbl> queryByIds(List<Long> ids) throws SQLException {
		if (CollectionUtils.isEmpty(ids)) {
			return new ArrayList<>();
		}
		SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
		sqlBuilder.selectAll().in(ID, ids, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
		return client.query(sqlBuilder, new DalHints());
	}
}