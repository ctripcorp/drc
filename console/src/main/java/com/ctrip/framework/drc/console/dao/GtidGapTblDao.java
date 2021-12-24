package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.GtidGapTbl;

import java.sql.SQLException;


/**
 * @author shb沈海波
 * @date 2020-03-02
 */
public class GtidGapTblDao extends AbstractDao<GtidGapTbl> {

	public GtidGapTblDao() throws SQLException {
		super(GtidGapTbl.class);
	}
}
