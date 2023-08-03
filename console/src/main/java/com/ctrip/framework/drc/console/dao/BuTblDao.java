package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
public class BuTblDao extends AbstractDao<BuTbl> {
	
	public BuTblDao() throws SQLException {
		super(BuTbl.class);
	}

}