package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DcTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class DcTblDao extends AbstractDao<DcTbl> {

	public DcTblDao() throws SQLException {
		super(DcTbl.class);
	}

}