package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2021-05-12
 */
public class ProxyTblDao extends AbstractDao<ProxyTbl> {

	public ProxyTblDao() throws SQLException {
		super(ProxyTbl.class);
	}
}
