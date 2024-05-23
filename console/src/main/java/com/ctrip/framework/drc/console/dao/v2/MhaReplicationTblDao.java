package com.ctrip.framework.drc.console.dao.v2;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/25 12:03
 */
@Repository
public class MhaReplicationTblDao extends AbstractDao<MhaReplicationTbl> {

    private static final String SRC_MHA_ID = "src_mha_id";
    private static final String DST_MHA_ID = "dst_mha_id";
    private static final String DELETED = "deleted";
    private static final String DATA_CHANGE_LAST_TIME = "datachange_lasttime";

    private static final boolean DESCENDING = false;
    public static final String DRC_STATUS = "drc_status";

    public MhaReplicationTblDao() throws SQLException {
        super(MhaReplicationTbl.class);
    }

    public List<MhaReplicationTbl> queryByPage(MhaReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().atPage(query.getPageIndex(), query.getPageSize())
                .orderBy(SRC_MHA_ID, DESCENDING)
                .orderBy(DATA_CHANGE_LAST_TIME, DESCENDING);
        this.buildQueryCondition(sqlBuilder, query);
        return client.query(sqlBuilder, new DalHints());
    }

    public int count(MhaReplicationQuery query) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder().selectCount();
        this.buildQueryCondition(sqlBuilder, query);
        return client.count(sqlBuilder, new DalHints()).intValue();
    }

    private void buildQueryCondition(SelectSqlBuilder sqlBuilder, MhaReplicationQuery query) throws SQLException {
        sqlBuilder.and()
                .inNullable(ID, query.getIdList(), Types.BIGINT).and()
                .notInNullable(ID, query.getNotInIdList(), Types.BIGINT).and()
                .inNullable(SRC_MHA_ID, query.getSrcMhaIdList(), Types.BIGINT).and()
                .inNullable(DST_MHA_ID, query.getDstMhaIdList(), Types.BIGINT).and()
                .equalNullable(DRC_STATUS, query.getDrcStatus(), Types.TINYINT).and()
                .leftBracket()
                .inNullable(SRC_MHA_ID, query.getRelatedMhaIdList(), Types.BIGINT).or()
                .inNullable(DST_MHA_ID, query.getRelatedMhaIdList(), Types.BIGINT)
                .rightBracket();
    }
    public List<MhaReplicationTbl> queryByRelatedMhaId(List<Long> relatedMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.selectAll().and()
                .leftBracket()
                .in(SRC_MHA_ID, relatedMhaId, Types.BIGINT)
                .or()
                .in(DST_MHA_ID, relatedMhaId, Types.BIGINT)
                .rightBracket();
        return client.query(sqlBuilder, new DalHints());
    }
    public MhaReplicationTbl queryByMhaId(Long srcMhaId, Long dstMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT).and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public MhaReplicationTbl queryByMhaId(Long srcMhaId, Long dstMhaId, Integer deleted) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT)
                .and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT)
                .and().equal(DELETED, deleted, Types.TINYINT);
        return client.queryFirst(sqlBuilder, new DalHints());
    }

    public Long insertOrReCover(Long srcMhaId, Long dstMhaId) throws SQLException {
        if (srcMhaId == null || dstMhaId == null) {
            throw ConsoleExceptionUtils.message("insertOrReCover mhaReplication, srcMhaId or dstMhaId is null");
        }
        MhaReplicationTbl mhaReplicationTbl = queryByMhaId(srcMhaId, dstMhaId);
        if (mhaReplicationTbl != null) {
            if (mhaReplicationTbl.getDeleted() == 0) {
                return mhaReplicationTbl.getId();
            }
            mhaReplicationTbl.setDeleted(0);
            update(new DalHints(), mhaReplicationTbl);
            return mhaReplicationTbl.getId();
        } else {
            mhaReplicationTbl = new MhaReplicationTbl();
            mhaReplicationTbl.setSrcMhaId(srcMhaId);
            mhaReplicationTbl.setDstMhaId(dstMhaId);
            mhaReplicationTbl.setDrcStatus(0);
            mhaReplicationTbl.setDeleted(0);
            return insertWithReturnId(mhaReplicationTbl);
        }
    }
    
    public Long insertWithReturnId(MhaReplicationTbl mhaReplicationTbl) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insert(new DalHints(), keyHolder, mhaReplicationTbl);
        return (Long) keyHolder.getKey();
    }

    public List<MhaReplicationTbl> queryByDstMhaId(long dstMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(DST_MHA_ID, dstMhaId, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<MhaReplicationTbl> queryBySamples(List<MhaReplicationTbl> samples) throws SQLException {
        if (CollectionUtils.isEmpty(samples)) {
            return Collections.emptyList();
        }
        return queryBySQL(buildSQL(samples));
    }


    private String buildSQL(List<MhaReplicationTbl> samples) {
        // e.g: (src_mha_id, dst_mha_id) IN ((13119, 13126), (13161, 13189));
        List<String> list = samples.stream().map(e -> String.format("(%d,%d)", e.getSrcMhaId(), e.getDstMhaId())).collect(Collectors.toList());
        return String.format("(%s, %s) in (%s) and deleted = 0", SRC_MHA_ID, DST_MHA_ID, String.join(",", list));
    }

    public List<MhaReplicationTbl> queryBySrcMhaId(Long srcMhaId) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(SRC_MHA_ID, srcMhaId, Types.BIGINT);
        return queryList(sqlBuilder);
    }
}
