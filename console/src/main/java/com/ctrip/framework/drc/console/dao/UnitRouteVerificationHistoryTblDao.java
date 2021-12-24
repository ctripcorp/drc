package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.UnitRouteVerificationHistoryTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2021-03-22
 */
public class UnitRouteVerificationHistoryTblDao extends AbstractDao<UnitRouteVerificationHistoryTbl> {

	public UnitRouteVerificationHistoryTblDao() throws SQLException {
		super(UnitRouteVerificationHistoryTbl.class);
	}

	public List<UnitRouteVerificationHistoryTbl> queryAll(boolean asc) throws SQLException {
		DalHints hints = DalHints.createIfAbsent(null);

		SelectSqlBuilder builder = new SelectSqlBuilder().selectAll().orderBy("id", asc);

		return client.query(builder, hints);
	}
}
