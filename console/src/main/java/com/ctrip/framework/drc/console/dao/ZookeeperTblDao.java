package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ZookeeperTbl;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
@Repository
public class ZookeeperTblDao extends AbstractDao<ZookeeperTbl> {

	public ZookeeperTblDao() throws SQLException {
		super(ZookeeperTbl.class);
	}

}