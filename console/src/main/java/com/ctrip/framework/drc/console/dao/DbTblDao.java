package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
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

	public List<DbTbl> queryByIds(List<Long> ids) throws SQLException {
		if (CollectionUtils.isEmpty(ids)) {
			return new ArrayList<>();
		}
		SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
		sqlBuilder.selectAll().in(ID, ids, Types.BIGINT).and().equal(DELETED, BooleanEnum.FALSE.getCode(), Types.TINYINT);
		return client.query(sqlBuilder, new DalHints());
	}
}