package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-09-20
 */
public class MhaTblDao extends AbstractDao<MhaTbl>{

	public MhaTblDao() throws SQLException {
		super(MhaTbl.class);
	}
	
	public List<MhaTbl> queryByDeleted(Integer deleted) throws SQLException {
		if (deleted == null) {
			throw new IllegalArgumentException("build sql: query ByDeleted,deleted is null ");
		}
		MhaTbl sample = new MhaTbl();
		sample.setDeleted(deleted);
		return this.queryBy(sample);
	}
	
	public MhaTbl queryByMhaName(String mhaName,Integer deleted) throws SQLException {
		if (StringUtils.isBlank(mhaName)) {
			throw new IllegalArgumentException("build sql: query MhaTbl ByMhaName, but name is empty ");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("mha_name", mhaName, Types.VARCHAR)
				.and().equal("deleted", deleted, Types.TINYINT, false);
		List<MhaTbl> mhaTbls = client.query(builder, new DalHints());
		// only one or null
		return mhaTbls.isEmpty() ? null : mhaTbls.get(0);
	}
	
	
	public List<MhaTbl> queryByDcId(Long dcId) throws SQLException {
		final SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll();
		if (null != dcId) {
			builder.and();
			builder.equal("dc_id", dcId, Types.BIGINT, false);
		}
		return client.query(builder, new DalHints());
	}

	public MhaTbl queryById(Long id) throws SQLException {
		SelectSqlBuilder sqlBuilder = initSqlBuilder();
		sqlBuilder.and().equal("id", id, Types.BIGINT);
		return client.queryFirst(sqlBuilder, new DalHints());
	}
}