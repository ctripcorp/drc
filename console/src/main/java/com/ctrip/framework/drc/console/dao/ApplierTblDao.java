package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ApplierTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ApplierTblDao extends AbstractDao<ApplierTbl> {

	public ApplierTblDao() throws SQLException {
		super(ApplierTbl.class);
	}

}