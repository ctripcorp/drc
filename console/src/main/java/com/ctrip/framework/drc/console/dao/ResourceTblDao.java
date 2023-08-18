package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ResourceTblDao extends AbstractDao<ResourceTbl> {

	private static final String TYPE = "type";
	private static final String IP = "ip";
	private static final String DC_ID = "dc_id";

	public ResourceTblDao() throws SQLException {
		super(ResourceTbl.class);
	}

	public List<ResourceTbl> queryByType(int type) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal(TYPE, type, Types.TINYINT);
		return queryList(sqlBuilder);
	}

	public List<ResourceTbl> queryByIps(List<String> ips) throws SQLException {
	    if (CollectionUtils.isEmpty(ips)) {
	    	return new ArrayList<>();
		}
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
	    sqlBuilder.and().in(IP, ips, Types.VARBINARY);
	    return queryList(sqlBuilder);
	}

	public List<ResourceTbl> queryByDcAndType(List<Long> dcIds, int type) throws SQLException {
		SelectSqlBuilder sqlbuilder = initSqlBuilder();
		sqlbuilder.and().inNullable(DC_ID, dcIds, Types.BIGINT)
				.and().equal(TYPE, type, Types.TINYINT);
		return queryList(sqlbuilder);
	}

}