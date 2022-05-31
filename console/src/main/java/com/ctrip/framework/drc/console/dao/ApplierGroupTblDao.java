package com.ctrip.framework.drc.console.dao;


import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ApplierGroupTblDao extends AbstractDao<ApplierGroupTbl>{
	
	public ApplierGroupTblDao() throws SQLException {
		super(ApplierGroupTbl.class);
	}

	
	
	public ApplierGroupTbl queryByMhaIdAndReplicatorGroupId(Long mhaId, Long replicatorGroupId,Integer deleted) throws SQLException {
		if (null == mhaId || null == replicatorGroupId) {
			throw new IllegalArgumentException("build sql: query ApplierGroupTbl ByMhaIdAndReplicatorGroupId, " +
					"but one is empty mhaId,replicatorGroupId");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("mha_id", mhaId, Types.BIGINT,false)
				.and().equal("replicator_group_id", replicatorGroupId, Types.BIGINT,false)
				.and().equal("deleted", deleted, Types.TINYINT, false);
		List<ApplierGroupTbl> applierGroupTbl = client.query(builder, new DalHints());
		// only one or null
		return applierGroupTbl.isEmpty() ? null : applierGroupTbl.get(0);
	}

    public Long upsertIfNotExist(Long srcReplicatorGroupId, Long destMhaId) throws SQLException {
		ApplierGroupTbl aGroupTbl = 
				queryByMhaIdAndReplicatorGroupId(destMhaId, srcReplicatorGroupId, BooleanEnum.FALSE.getCode());
		if (aGroupTbl != null) {
			return aGroupTbl.getId();
		} else {
			ApplierGroupTbl aGroupTblDeleted =
					queryByMhaIdAndReplicatorGroupId(destMhaId, srcReplicatorGroupId, BooleanEnum.TRUE.getCode());
			if (aGroupTblDeleted != null) {
				aGroupTblDeleted.setDeleted(BooleanEnum.FALSE.getCode());
				update(aGroupTblDeleted);
				return aGroupTblDeleted.getId();
			} else {
				KeyHolder keyHolder = new KeyHolder();
				ApplierGroupTbl applierGroupTbl = new ApplierGroupTbl();
				applierGroupTbl.setReplicatorGroupId(srcReplicatorGroupId);
				applierGroupTbl.setMhaId(destMhaId);
				insert(new DalHints(), keyHolder, applierGroupTbl);
				return (Long) keyHolder.getKey();
			}
		}
	}
	
	
}