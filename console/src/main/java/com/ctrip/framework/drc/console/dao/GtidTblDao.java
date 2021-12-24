package com.ctrip.framework.drc.console.dao;


import com.ctrip.framework.drc.console.dao.entity.GtidTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-03-02
 */
public class GtidTblDao extends AbstractDao<GtidTbl> {

	public GtidTblDao() throws SQLException {
		super(GtidTbl.class);
	}
}
