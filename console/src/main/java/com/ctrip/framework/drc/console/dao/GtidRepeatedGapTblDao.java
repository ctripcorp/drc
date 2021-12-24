package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.GtidRepeatedGapTbl;

import java.sql.SQLException;


/**
 * @author shb沈海波
 * @date 2020-03-02
 */
public class GtidRepeatedGapTblDao extends AbstractDao<GtidRepeatedGapTbl> {

	public GtidRepeatedGapTblDao() throws SQLException {
		super(GtidRepeatedGapTbl.class);
	}
}
