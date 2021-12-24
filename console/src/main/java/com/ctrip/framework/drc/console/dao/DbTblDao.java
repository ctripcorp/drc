package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class DbTblDao extends AbstractDao<DbTbl> {

	public DbTblDao() throws SQLException {
		super(DbTbl.class);
	}

}