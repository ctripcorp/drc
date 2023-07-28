package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
public class BuTblDao extends AbstractDao<BuTbl> {
	
	public BuTblDao() throws SQLException {
		super(BuTbl.class);
	}

	@Override
	public List<BuTbl> queryAll() throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.selectAll().orderBy("id", true);
		return client.query(sqlBuilder, new DalHints());
	}
}