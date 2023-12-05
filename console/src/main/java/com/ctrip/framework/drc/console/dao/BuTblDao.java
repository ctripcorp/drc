package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;

/**
 * @author shb沈海波
 * @date 2020-08-12
 */
@Repository
public class BuTblDao extends AbstractDao<BuTbl> {
	
	public BuTblDao() throws SQLException {
		super(BuTbl.class);
	}

	public BuTbl queryByBuName(String buName) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal("bu_name", buName, Types.VARCHAR);
		return queryOne(sqlBuilder);
	}

	public Long upsert(String buName) throws SQLException {
		BuTbl buTbl = queryAll().stream().filter(p -> p.getBuName().equalsIgnoreCase(buName)).findFirst().orElse(null);
		if(null == buTbl) {
			return insertBu(buName);
		} else if (BooleanEnum.TRUE.getCode().equals(buTbl.getDeleted())) {
			buTbl.setDeleted(BooleanEnum.FALSE.getCode());
			update(buTbl);
		}
		return buTbl.getId();
	}

	public Long insertBu(String buName) throws SQLException {
		BuTbl pojo = BuTbl.createBuPojo(buName);
		KeyHolder keyHolder = new KeyHolder();
		insert(new DalHints(), keyHolder, pojo);
		return (Long) keyHolder.getKey();
	}
}