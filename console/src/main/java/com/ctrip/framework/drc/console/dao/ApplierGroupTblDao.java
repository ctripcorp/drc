package com.ctrip.framework.drc.console.dao;


import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ApplierGroupTblDao extends AbstractDao<ApplierGroupTbl>{
	
	public ApplierGroupTblDao() throws SQLException {
		super(ApplierGroupTbl.class);
	}

}