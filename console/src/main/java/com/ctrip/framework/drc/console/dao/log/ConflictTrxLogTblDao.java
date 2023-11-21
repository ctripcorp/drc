package com.ctrip.framework.drc.console.dao.log;

import com.ctrip.framework.drc.console.dao.AbstractDao;
import com.ctrip.framework.drc.console.dao.log.entity.ConflictTrxLogTbl;
import com.ctrip.framework.drc.console.param.log.ConflictTrxLogQueryParam;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.sqlbuilder.MatchPattern;
import com.ctrip.platform.dal.dao.sqlbuilder.SelectSqlBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/9/26 14:21
 */
@Repository
public class ConflictTrxLogTblDao extends AbstractDao<ConflictTrxLogTbl> {

    private static final String SRC_MHA_NAME = "src_mha_name";
    private static final String DST_MHA_NAME = "dst_mha_name";
    private static final String DB = "db";
    private static final String GTID = "gtid";
    private static final String HANDLE_TIME = "handle_time";
    private static final String TRX_RESULT = "trx_result";
    private static final String WHERE_SQL = "handle_time >= ? and handle_time <= ?";

    public ConflictTrxLogTblDao() throws SQLException {
        super(ConflictTrxLogTbl.class);
    }

    public List<ConflictTrxLogTbl> queryByParam(ConflictTrxLogQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectCount();
        int count = client.count(sqlBuilder, new DalHints()).intValue();
        param.getPageReq().setTotalCount(count);

        sqlBuilder = buildSqlBuilder(param);
        sqlBuilder.selectAll().atPage(param.getPageReq().getPageIndex(), param.getPageReq().getPageSize()).orderBy(HANDLE_TIME, false);
        return queryList(sqlBuilder);
    }

    public ConflictTrxLogTbl queryByGtid(String gtid) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(GTID, gtid, Types.VARCHAR);
        return queryOne(sqlBuilder);
    }

    public ConflictTrxLogTbl queryByGtid(String gtid, Long beginHandleTime, Long endHandleTime) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        sqlBuilder.and().equal(GTID, gtid, Types.VARCHAR)
                .and().greaterThan(HANDLE_TIME, beginHandleTime, Types.BIGINT)
                .and().lessThan(HANDLE_TIME, endHandleTime, Types.BIGINT);
        return queryOne(sqlBuilder);
    }

    private SelectSqlBuilder buildSqlBuilder(ConflictTrxLogQueryParam param) throws SQLException {
        SelectSqlBuilder sqlBuilder = initSqlBuilder();
        if (!param.isAdmin()) {
            sqlBuilder.and().in(DB, param.getDbsWithPermission(), Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getDb())) {
            sqlBuilder.and().equal(DB, param.getDb(), Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getSrcMhaName())) {
            sqlBuilder.and().equal(SRC_MHA_NAME, param.getSrcMhaName(), Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getDstMhaName())) {
            sqlBuilder.and().equal(DST_MHA_NAME, param.getDstMhaName(), Types.VARCHAR);
        }
        if (StringUtils.isNotBlank(param.getGtId())) {
            sqlBuilder.and().equal(GTID, param.getGtId(), Types.VARCHAR);
        }
        sqlBuilder.and().equalNullable(TRX_RESULT, param.getTrxResult(), Types.TINYINT);

        if (param.getBeginHandleTime() != null && param.getBeginHandleTime() > 0L) {
            sqlBuilder.and().greaterThan(HANDLE_TIME, param.getBeginHandleTime(), Types.BIGINT);
        }
        if (param.getEndHandleTime() != null && param.getEndHandleTime() > 0L) {
            sqlBuilder.and().lessThan(HANDLE_TIME, param.getEndHandleTime(), Types.BIGINT);
        }

        return sqlBuilder;
    }

    public List<ConflictTrxLogTbl> queryByHandleTime(long beginTime, long endTime) throws SQLException {
        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
        sqlBuilder.selectAll().greaterThan(HANDLE_TIME, beginTime, Types.BIGINT)
                .and().lessThan(HANDLE_TIME, endTime, Types.BIGINT);
        return queryList(sqlBuilder);
    }

    public List<ConflictTrxLogTbl> batchInsertWithReturnId(List<ConflictTrxLogTbl> conflictTrxLogTbls) throws SQLException {
        KeyHolder keyHolder = new KeyHolder();
        insertWithKeyHolder(keyHolder, conflictTrxLogTbls);
        List<Number> idList = keyHolder.getIdList();
        int size = conflictTrxLogTbls.size();
        for (int i = 0; i < size; i++) {
            conflictTrxLogTbls.get(i).setId((Long) idList.get(i));
        }

        return conflictTrxLogTbls;
    }

    public int batchDeleteByHandleTime(long beginTime, long endTime) throws SQLException {
        return client.delete(WHERE_SQL, new DalHints(), beginTime, endTime);
    }
}
