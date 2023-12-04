package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
@Repository
public class BuTblDao extends AbstractDao<BuTbl> {
	
	public BuTblDao() throws SQLException {
		super(BuTbl.class);
	}

	public BuTbl queryByBuName(String buName) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal("bu_name", buName, Types.VARCHAR);
		return queryOne(sqlBuilder);
	}
}