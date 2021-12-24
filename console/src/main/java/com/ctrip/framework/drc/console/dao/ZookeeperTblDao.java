package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ZookeeperTbl;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
public class ZookeeperTblDao extends AbstractDao<ZookeeperTbl> {

	public ZookeeperTblDao() throws SQLException {
		super(ZookeeperTbl.class);
	}

}