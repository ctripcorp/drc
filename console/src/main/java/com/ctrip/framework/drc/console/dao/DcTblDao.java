package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class DcTblDao extends AbstractDao<DcTbl> {

	private static final String REGION_NAME = "region_name";

	public DcTblDao() throws SQLException {
		super(DcTbl.class);
	}

	public List<DcTbl> queryByRegionName(String regionName) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal(REGION_NAME, regionName, Types.VARCHAR);
		return queryList(sqlBuilder);
	}

}