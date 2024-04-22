package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-11
 */
@Repository
public class DcTblDao extends AbstractDao<DcTbl> {

	private static final String REGION_NAME = "region_name";
	private static final String DC_NAME = "dc_name";

	public DcTblDao() throws SQLException {
		super(DcTbl.class);
	}

	public List<DcTbl> queryByRegionName(String regionName) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal(REGION_NAME, regionName, Types.VARCHAR);
		return queryList(sqlBuilder);
	}

	public DcTbl queryByDcName(String dcName) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal(DC_NAME, dcName, Types.VARCHAR);
		return queryOne(sqlBuilder);
	}

	public Long upsert(String dc) throws SQLException {
		DcTbl dcTbl = queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase(dc)).findFirst().orElse(null);
		if (null == dcTbl) {
			return insertDc(dc);
		} else if (BooleanEnum.TRUE.getCode().equals(dcTbl.getDeleted())){
			dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
			update(dcTbl);
		}
		return dcTbl.getId();
	}

	public Long upsert(String dc, String region) throws SQLException {
		DcTbl dcTbl = queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase(dc)).findFirst().orElse(null);
		if (null == dcTbl) {
			return insertDc(dc, region);
		} else if (BooleanEnum.TRUE.getCode().equals(dcTbl.getDeleted())){
			dcTbl.setDeleted(BooleanEnum.FALSE.getCode());
			update(dcTbl);
		}
		return dcTbl.getId();
	}

	public Long insertDc(String dcName, String region) throws SQLException {
		DcTbl pojo = new DcTbl();
		pojo.setDcName(dcName);
		pojo.setRegionName(region);

		KeyHolder keyHolder = new KeyHolder();
		insert(new DalHints(), keyHolder, pojo);
		return (Long) keyHolder.getKey();
	}

	public Long insertDc(String dcName) throws SQLException {
		DcTbl pojo = DcTbl.createDcPojo(dcName);
		KeyHolder keyHolder = new KeyHolder();
		insert(new DalHints(), keyHolder, pojo);
		return (Long) keyHolder.getKey();
	}

}