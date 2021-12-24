package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class MhaGroupTblDao extends AbstractDao<MhaGroupTbl> {

	public MhaGroupTblDao() throws SQLException {
		super(MhaGroupTbl.class);
	}

}