package com.ctrip.framework.drc.console.dao;

import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author shb沈海波
 * @date 2020-08-28
 */
public class ReplicatorGroupTblDao  extends AbstractDao<ReplicatorGroupTbl> {

	private static final Logger logger = LoggerFactory.getLogger(ReplicatorGroupTblDao.class);

	public ReplicatorGroupTblDao() throws SQLException {
		super(ReplicatorGroupTbl.class);
	}
	
	//mhaId which replicator fetch binlog
	public List<ReplicatorGroupTbl> queryByMhaIds (List<Long> mhaIds , Integer deleted) throws SQLException {
		if (CollectionUtils.isEmpty(mhaIds)) {
			throw new IllegalArgumentException("build sql: query ReplicatorGroupTbl ByMhaIds, but mhaIds is empty.");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("deleted", deleted, Types.TINYINT, false).
				and().in("mha_id", mhaIds, Types.BIGINT, false);
		return client.query(builder, new DalHints());
	}

	public ReplicatorGroupTbl queryByMhaId(Long mhaId) throws SQLException {
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("mha_id", mhaId, Types.BIGINT);
		return queryOne(builder);
	}

	public ReplicatorGroupTbl queryByMhaId(Long mhaId, Integer deleted) throws SQLException {
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("mha_id", mhaId, Types.BIGINT)
		.and().equal("deleted", deleted, Types.TINYINT, false);
		return queryOne(builder);
	}

	//mhaId which replicator fetch binlog
	public Long upsertIfNotExist(Long mhaId) throws SQLException {
		if (mhaId == null) {
			throw new IllegalArgumentException("insertOrReCover ReplicatorGroupTbl, mhaId is null");
		}
		SelectSqlBuilder builder = new SelectSqlBuilder();
		builder.selectAll().equal("mha_id", mhaId, Types.BIGINT, false);
		List<ReplicatorGroupTbl> RGroups = client.query(builder, new DalHints());
		if (CollectionUtils.isEmpty(RGroups)) {
			logger.info("[[dao=replicatorGroupDao,mhaId={}]] insert RGroup", mhaId);
			return insertRGroup(mhaId);
		} else {
			ReplicatorGroupTbl replicatorGroupTbl = RGroups.get(0);
			if (BooleanEnum.TRUE.getCode().equals(replicatorGroupTbl.getDeleted())) {
				logger.info("[[dao=replicatorGroupDao,mhaId={}]] update RGroup", mhaId);
				replicatorGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
				update(replicatorGroupTbl);
			}
			return replicatorGroupTbl.getId();
		}
	}

	public Long insertRGroup(long mhaId) throws SQLException {
		KeyHolder keyHolder = new KeyHolder();
		ReplicatorGroupTbl rGroupPojo = new ReplicatorGroupTbl();
		rGroupPojo.setMhaId(mhaId);
		insert(new DalHints(), keyHolder, rGroupPojo);
		return (Long) keyHolder.getKey();
	}
	
}